// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/cli/tui/internal/model"
	"cloudeng.io/cmd/idu/cli/tui/internal/widgets"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmd/idu/stats"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// The names of the top-N tabs.
const (
	bytesTab    = "Bytes"
	storageTab  = "Storage"
	filesTab    = "Files"
	subdirsTab  = "Prefixes"
	usersTab    = "Users"
	groupsTab   = "Groups"
	hotspotsTab = "Hotspots"
)

// topNDelay is how long the highlight must remain on a prefix before its
// top-N rankings are loaded, so that scrolling through the tree does not
// start a computation for every prefix passed over.
const topNDelay = 300 * time.Millisecond

// nodeRef is the reference stored with each tree node.
type nodeRef struct {
	node   model.Node
	loaded bool
}

type viewScreen struct {
	u       *ui
	layout  *tview.Flex
	upper   *tview.Flex
	tree    *tview.TreeView
	summary *tview.TextView
	tabs    *widgets.Tabs
	tables  map[string]*tview.Table

	bottomWeight int
	maximized    bool

	mu sync.Mutex
	// cancel cancels the requests for the previously highlighted prefix.
	cancel context.CancelFunc
	// generation is incremented whenever the highlight moves so that the
	// results of stale requests are discarded.
	generation int
}

func newViewScreen(u *ui) *viewScreen {
	v := &viewScreen{u: u, bottomWeight: 3, tables: map[string]*tview.Table{}}
	v.tree = tview.NewTreeView()
	v.tree.SetBorder(true).SetTitle(" " + u.cfg.fs.Location() + " ")
	v.tree.SetChangedFunc(v.highlighted)
	v.tree.SetSelectedFunc(v.toggle)
	v.summary = tview.NewTextView().SetDynamicColors(true).SetWrap(true)
	v.summary.SetBorder(true).SetTitle(" Summary ")
	v.tabs = widgets.NewTabs(u.app)
	v.tabs.SetBorder(true).SetTitle(" Top-N ")
	for _, name := range []string{bytesTab, storageTab, filesTab, subdirsTab, usersTab, groupsTab, hotspotsTab} {
		t := tview.NewTable().SetSelectable(true, false).SetFixed(1, 0)
		if name != usersTab && name != groupsTab && name != hotspotsTab {
			t.SetSelectedFunc(func(row, _ int) { v.gotoRow(t, row) })
		}
		v.tables[name] = t
		v.tabs.AddTab(name, t)
	}
	v.upper = tview.NewFlex().
		AddItem(v.tree, 0, 1, true).
		AddItem(v.summary, 0, 1, false)
	v.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(v.upper, 0, topWeight, true).
		AddItem(v.tabs, 0, v.bottomWeight, false)
	v.reset()
	return v
}

// reset clears the tree, summary and tables.
func (v *viewScreen) reset() {
	root := tview.NewTreeNode(v.u.cfg.fs.Location()).SetReference(&nodeRef{node: model.Node{Path: v.u.cfg.fs.Location()}})
	v.tree.SetRoot(root).SetCurrentNode(root)
	v.summary.SetText("loading...")
	for _, t := range v.tables {
		t.Clear()
	}
}

func (v *viewScreen) currentPath() string {
	if n := v.tree.GetCurrentNode(); n != nil {
		return n.GetReference().(*nodeRef).node.Path
	}
	return v.u.cfg.fs.Location()
}

// load loads the root's children and highlights target, expanding its
// ancestors as necessary. It must be called on the tui's goroutine.
func (v *viewScreen) load(target string) {
	root := v.tree.GetRoot()
	v.expandTo(root, target)
}

// home highlights the root.
func (v *viewScreen) home() {
	root := v.tree.GetRoot()
	v.tree.SetCurrentNode(root)
	v.highlighted(root)
}

// level is the children of a prefix.
type level struct {
	path     string
	children []model.Node
}

// expandTo loads the children of node and its descendants that are
// ancestors of target, in the background, and then highlights target.
func (v *viewScreen) expandTo(node *tview.TreeNode, target string) {
	src := v.u.source()
	if src == nil {
		return
	}
	path := node.GetReference().(*nodeRef).node.Path
	go func() {
		levels, err := fetchLevels(v.u.ctx, src, path, target)
		if err != nil {
			v.u.showError("failed to read the prefixes in %v", err)
			return
		}
		v.u.app.QueueUpdateDraw(func() { v.applyLevels(node, target, levels) })
	}()
}

// fetchLevels returns the children of path and of each of its descendants
// that is an ancestor of target.
func fetchLevels(ctx context.Context, src model.Source, path, target string) ([]level, error) {
	var levels []level
	for {
		children, err := src.Children(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", path, err)
		}
		levels = append(levels, level{path, children})
		next := ""
		for _, c := range children {
			if c.Path == target || isAncestor(c.Path, target) {
				next = c.Path
			}
		}
		if next == "" || next == target {
			return levels, nil
		}
		path = next
	}
}

// applyLevels adds the children in levels to node and its descendants and
// highlights target, or its closest ancestor.
func (v *viewScreen) applyLevels(node *tview.TreeNode, target string, levels []level) {
	current := node
	for _, l := range levels {
		v.setChildren(current, l.children)
		current.SetExpanded(true)
		next := childFor(current, target)
		if next == nil {
			break
		}
		current = next
	}
	v.tree.SetCurrentNode(current)
	v.highlighted(current)
}

// childFor returns the child of node that is, or is an ancestor of,
// target.
func childFor(node *tview.TreeNode, target string) *tview.TreeNode {
	for _, c := range node.GetChildren() {
		p := c.GetReference().(*nodeRef).node.Path
		if p == target || isAncestor(p, target) {
			return c
		}
	}
	return nil
}

func isAncestor(parent, path string) bool {
	return len(path) > len(parent) && strings.HasPrefix(path, parent) &&
		(strings.HasSuffix(parent, "/") || strings.HasSuffix(parent, "\\") || path[len(parent)] == '/' || path[len(parent)] == '\\')
}

// setChildren replaces the children of node, which have been loaded.
func (v *viewScreen) setChildren(node *tview.TreeNode, children []model.Node) {
	ref := node.GetReference().(*nodeRef)
	if ref.loaded {
		return
	}
	ref.loaded = true
	node.ClearChildren()
	for _, c := range children {
		// Prefixes without children are distinguished by their label, see
		// label, rather than by color: palette colors, eg. tcell.ColorGray,
		// which is color 8 (bright black), may be the same as the
		// terminal's background color and hence invisible.
		node.AddChild(tview.NewTreeNode(v.label(c)).SetReference(&nodeRef{node: c}))
	}
}

func (v *viewScreen) label(n model.Node) string {
	marker := " "
	if n.HasChildren() {
		marker = "+"
	}
	t := n.Totals()
	return fmt.Sprintf("%s %s  %s (%s)", marker, n.Name, strings.TrimSpace(v.u.cfg.env.FmtSize(t.StorageBytes)), strings.TrimSpace(cli.FmtCount(t.Files)))
}

// toggle expands, loading its children if necessary, or collapses node.
func (v *viewScreen) toggle(node *tview.TreeNode) {
	ref := node.GetReference().(*nodeRef)
	if ref.loaded {
		node.SetExpanded(!node.IsExpanded())
		return
	}
	src := v.u.source()
	if src == nil {
		return
	}
	go func() {
		children, err := src.Children(v.u.ctx, ref.node.Path)
		if err != nil {
			v.u.showError("failed to read the prefixes in %v: %v", ref.node.Path, err)
			return
		}
		v.u.app.QueueUpdateDraw(func() {
			v.setChildren(node, children)
			node.SetExpanded(true)
		})
	}()
}

// highlighted is called when the highlight moves to node. It loads the
// summary for node immediately and its top-N rankings after topNDelay.
func (v *viewScreen) highlighted(node *tview.TreeNode) {
	src := v.u.source()
	if src == nil || node == nil {
		return
	}
	path := node.GetReference().(*nodeRef).node.Path
	v.mu.Lock()
	if v.cancel != nil {
		v.cancel()
	}
	ctx, cancel := context.WithCancel(v.u.ctx)
	v.cancel = cancel
	v.generation++
	gen := v.generation
	v.mu.Unlock()
	go v.loadSummary(ctx, src, gen, path)
	go v.loadTopN(ctx, src, gen, path)
}

func (v *viewScreen) current(gen int) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return gen == v.generation
}

func (v *viewScreen) loadSummary(ctx context.Context, src model.Source, gen int, path string) {
	sum, err := src.Summary(ctx, path)
	v.u.app.QueueUpdateDraw(func() {
		if !v.current(gen) {
			return
		}
		if err != nil {
			v.summary.SetText(fmt.Sprintf("[red]%v", err))
			return
		}
		v.summary.SetText(formatSummary(v.u.cfg.env, sum)).ScrollToBeginning()
	})
}

func (v *viewScreen) loadTopN(ctx context.Context, src model.Source, gen int, path string) {
	select {
	case <-time.After(topNDelay):
	case <-ctx.Done():
		return
	}
	v.u.app.QueueUpdateDraw(func() {
		if v.current(gen) {
			v.tabs.SetTitle(fmt.Sprintf(" Top-N beneath %v: computing... ", path))
		}
	})
	start := time.Now()
	top, err := src.TopN(ctx, path, v.u.cfg.topN)
	if ctx.Err() != nil {
		return
	}
	v.u.app.QueueUpdateDraw(func() {
		if !v.current(gen) {
			return
		}
		if err != nil {
			v.tabs.SetTitle(fmt.Sprintf(" Top-N beneath %v: %v ", path, err))
			return
		}
		v.tabs.SetTitle(fmt.Sprintf(" Top-N beneath %v ", path))
		v.fillTables(top)
		v.u.status.SetText(fmt.Sprintf("top-N: %v (%v)", top.Source, time.Since(start).Truncate(time.Millisecond)))
	})
}

func formatSummary(env *cli.Env, s model.Summary) string {
	var b strings.Builder
	size := func(v int64) string { return strings.TrimSpace(env.FmtSize(v)) }
	count := func(v int64) string { return strings.TrimSpace(cli.FmtCount(v)) }
	totals := func(t stats.Totals) {
		fmt.Fprintf(&b, "  bytes      %v\n", size(t.Bytes))
		fmt.Fprintf(&b, "  storage    %v\n", size(t.StorageBytes))
		fmt.Fprintf(&b, "  files      %v\n", count(t.Files))
		fmt.Fprintf(&b, "  prefixes   %v\n", count(t.Prefix))
		fmt.Fprintf(&b, "  hardlinks  %v\n", count(t.Hardlinks))
		if t.SparseBytes > 0 {
			fmt.Fprintf(&b, "  sparse     %v in %v files\n", size(t.SparseBytes), count(t.SparseFiles))
		}
	}
	fmt.Fprintf(&b, "[::b]%v[::-]\n\n", tview.Escape(s.Path))
	if s.HasSubtree {
		fmt.Fprintf(&b, "[::b]Subtree[::-]\n")
		totals(s.Subtree)
	} else {
		fmt.Fprintf(&b, "[::d]subtree totals unavailable, see analyze[::-]\n")
	}
	fmt.Fprintf(&b, "\n[::b]This directory's own files[::-]\n")
	totals(s.Direct)
	ids := func(title string, t stats.PerIDTotals, name func(int64) string) {
		if len(t) == 0 {
			return
		}
		fmt.Fprintf(&b, "\n[::b]%v[::-]\n", title)
		for _, id := range t {
			fmt.Fprintf(&b, "  %-12v %v in %v files\n", tview.Escape(name(id.ID)), size(id.Bytes), count(id.Files))
		}
	}
	ids("Users", s.Users, usernames.Manager.NameForUID)
	ids("Groups", s.Groups, usernames.Manager.NameForGID)
	if s.HasInfo {
		fmt.Fprintf(&b, "\n[::b]Directory[::-]\n  owner      %v:%v\n  mode       %v\n  modified   %v\n",
			tview.Escape(usernames.Manager.NameForUID(s.Info.XAttr.UID)), tview.Escape(usernames.Manager.NameForGID(s.Info.XAttr.GID)),
			s.Info.Mode(), s.Info.ModTime().Format(time.DateTime))
	}
	return b.String()
}

func header(t *tview.Table, titles ...string) {
	t.Clear()
	for i, title := range titles {
		t.SetCell(0, i, tview.NewTableCell(title).SetAttributes(tcell.AttrBold).SetSelectable(false))
	}
}

func (v *viewScreen) fillTables(top model.TopN) {
	size := func(n int64) string { return strings.TrimSpace(v.u.cfg.env.FmtSize(n)) }
	count := func(n int64) string { return strings.TrimSpace(cli.FmtCount(n)) }
	prefixes := func(name, metric string, r []model.Ranked[string], format func(int64) string) {
		t := v.tables[name]
		header(t, metric, "Prefix")
		for i, e := range r {
			t.SetCell(i+1, 0, tview.NewTableCell(format(e.Value)).SetAlign(tview.AlignRight))
			t.SetCell(i+1, 1, tview.NewTableCell(e.Item).SetReference(e.Item).SetExpansion(1))
		}
		t.ScrollToBeginning()
	}
	prefixes(bytesTab, "Bytes", top.Bytes, size)
	prefixes(storageTab, "Storage", top.Storage, size)
	prefixes(filesTab, "Files", top.Files, count)
	prefixes(subdirsTab, "Prefixes", top.Subdirs, count)
	ids := func(name, what string, r []model.Ranked[int64], nameFor func(int64) string) {
		t := v.tables[name]
		header(t, "Bytes", what)
		for i, e := range r {
			t.SetCell(i+1, 0, tview.NewTableCell(size(e.Value)).SetAlign(tview.AlignRight))
			t.SetCell(i+1, 1, tview.NewTableCell(nameFor(e.Item)).SetExpansion(1))
		}
	}
	ids(usersTab, "User", top.Users, usernames.Manager.NameForUID)
	ids(groupsTab, "Group", top.Groups, usernames.Manager.NameForGID)
	v.fillHotspots(top)
}

func (v *viewScreen) fillHotspots(top model.TopN) {
	size := func(n int64) string { return strings.TrimSpace(v.u.cfg.env.FmtSize(n)) }
	t := v.tables[hotspotsTab]
	if !top.HasHotspots {
		t.Clear()
		t.SetCell(0, 0, tview.NewTableCell("The hotspots are not included in these stats, use View > Refresh after stats view --hotspots, or stats compute --scan.").SetSelectable(false))
		return
	}
	header(t, "Distributed", "Max leaf", "Amplification", "Dirs", "Prefix")
	for i, ds := range top.Hotspots {
		row := []string{size(ds.DistributedBytes), size(ds.MaxLeafBytes), fmt.Sprintf("%.1fx", ds.BytesAmplification), strings.TrimSpace(cli.FmtCount(ds.SubtreeDirs)), ds.Prefix}
		for j, c := range row {
			cell := tview.NewTableCell(c)
			if j < 4 {
				cell.SetAlign(tview.AlignRight)
			} else {
				cell.SetExpansion(1)
			}
			t.SetCell(i+1, j, cell)
		}
	}
}

// gotoRow highlights the prefix in row of t in the tree.
func (v *viewScreen) gotoRow(t *tview.Table, row int) {
	cell := t.GetCell(row, 1)
	if path, ok := cell.GetReference().(string); ok {
		v.expandTo(v.tree.GetRoot(), path)
		v.u.app.SetFocus(v.tree)
	}
}

// inBottomPane returns true if the focus is in the top-N pane.
func (v *viewScreen) inBottomPane() bool {
	return v.tabs.HasFocus()
}

// focusOrder is the order in which tab cycles the focus.
func (v *viewScreen) focusOrder() []tview.Primitive {
	return []tview.Primitive{v.tree, v.summary, v.tables[v.tabs.Current()]}
}

func (v *viewScreen) cycleFocus(delta int) {
	order := v.focusOrder()
	focus := v.u.app.GetFocus()
	i := 0
	for j, p := range order {
		if p == focus {
			i = j
		}
	}
	v.u.app.SetFocus(order[(i+delta+len(order))%len(order)])
}

// resize changes the proportion of the screen used by the top-N pane.
func (v *viewScreen) resize(delta int) {
	v.maximized = false
	v.bottomWeight = min(max(v.bottomWeight+delta, minBottomWeight), maxBottomWeight)
	v.layout.ResizeItem(v.upper, 0, topWeight)
	v.layout.ResizeItem(v.tabs, 0, v.bottomWeight)
}

// toggleMaximized toggles the top-N pane between its size and the full
// height of the view.
func (v *viewScreen) toggleMaximized() {
	v.maximized = !v.maximized
	if v.maximized {
		v.layout.ResizeItem(v.upper, 0, 0)
		v.layout.ResizeItem(v.tabs, 0, 1)
		v.u.app.SetFocus(v.tables[v.tabs.Current()])
		return
	}
	v.resize(0)
	v.u.app.SetFocus(v.tree)
}

func (v *viewScreen) input(ev *tcell.EventKey) *tcell.EventKey {
	ctrl := ev.Modifiers()&tcell.ModCtrl != 0
	switch ev.Key() {
	case tcell.KeyTab, tcell.KeyRight:
		if v.tabs.IsActive() {
			// Left and right select the tabs in the tab bar.
			return ev
		}
		v.cycleFocus(1)
		return nil
	case tcell.KeyBacktab, tcell.KeyLeft:
		if v.tabs.IsActive() {
			return ev
		}
		v.cycleFocus(-1)
		return nil
	case tcell.KeyUp:
		if ctrl {
			v.resize(1)
			return nil
		}
		return v.up(ev)
	case tcell.KeyDown:
		if ctrl {
			v.resize(-1)
			return nil
		}
	case tcell.KeyRune:
		return v.runeInput(ev)
	}
	return ev
}

// up moves the focus to whatever is above the focused primitive if it is
// at its top: the top-N table's tab bar, and otherwise the menu bar.
func (v *viewScreen) up(ev *tcell.EventKey) *tcell.EventKey {
	focus := v.u.app.GetFocus()
	switch {
	case v.tabs.IsActive():
		v.u.menu.Activate()
	case focus == v.tree:
		if v.tree.GetCurrentNode() != v.tree.GetRoot() {
			return ev
		}
		v.u.menu.Activate()
	case focus == v.summary:
		if row, _ := v.summary.GetScrollOffset(); row > 0 {
			return ev
		}
		v.u.menu.Activate()
	case focus == v.tables[v.tabs.Current()]:
		// Row 0 is the table's header, which cannot be selected.
		if row, _ := v.tables[v.tabs.Current()].GetSelection(); row > 1 {
			return ev
		}
		v.tabs.Activate()
	default:
		return ev
	}
	return nil
}

func (v *viewScreen) runeInput(ev *tcell.EventKey) *tcell.EventKey {
	switch ev.Rune() {
	case '[':
		v.tabs.Prev()
	case ']':
		v.tabs.Next()
	case '+':
		v.resize(1)
	case '-':
		v.resize(-1)
	case 'z':
		v.toggleMaximized()
	case 'r':
		v.u.refresh()
	default:
		return ev
	}
	// Keep the focus on the current tab's table if a table had it.
	for _, t := range v.tables {
		if v.u.app.GetFocus() == t {
			v.u.app.SetFocus(v.tables[v.tabs.Current()])
			break
		}
	}
	return nil
}
