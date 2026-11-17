// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

// SortBy determines the order in which a directory's children are listed
// and which of their totals is displayed.
type SortBy int

const (
	ByStorage SortBy = iota
	ByBytes
	ByFiles
	ByName
	numSortBy
)

func (s SortBy) String() string {
	switch s {
	case ByStorage:
		return "storage"
	case ByBytes:
		return "bytes"
	case ByFiles:
		return "files"
	case ByName:
		return "name"
	}
	return "unknown"
}

// value returns the total for e that s sorts by, storage for ByName.
func (s SortBy) value(e Entry) int64 {
	switch s {
	case ByBytes:
		return e.Tree.Bytes
	case ByFiles:
		return e.Tree.Files
	}
	return e.Tree.StorageBytes
}

func (s SortBy) sort(entries []Entry) {
	slices.SortStableFunc(entries, func(a, b Entry) int {
		if s == ByName {
			return strings.Compare(a.Name, b.Name)
		}
		if va, vb := s.value(a), s.value(b); va != vb {
			if va > vb {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
}

// Options configures the browser.
type Options struct {
	// FormatBytes and FormatCount are used to display sizes and counts,
	// allowing the browser to match the formatting used by idu's other
	// commands.
	FormatBytes func(int64) string
	FormatCount func(int64) string
}

// level is one step on the path from the root to the current directory.
// The cursor and scroll offset are retained so that returning to a
// directory restores the previous selection.
type level struct {
	path           string
	cursor, offset int
}

// listingMsg delivers the result of Source.List.
type listingMsg struct {
	path    string
	listing Listing
	err     error
}

// Model is the Bubble Tea model for the browser. Navigation is always
// relative to the root it was created with: descending pushes a level,
// ascending pops one, so the browser never leaves the configured prefix.
type Model struct {
	ctx  context.Context
	src  Source
	opts Options

	stack   []level            // root first, current directory last.
	cache   map[string]Listing // listings, sorted by sortBy.
	sortBy  SortBy
	loading map[string]bool // listings currently being loaded.
	err     error           // error loading the current directory.

	width, height int
}

// New returns a browser rooted at root that obtains listings from src.
func New(ctx context.Context, src Source, root string, opts Options) *Model {
	if opts.FormatBytes == nil {
		opts.FormatBytes = formatBytes
	}
	if opts.FormatCount == nil {
		opts.FormatCount = func(n int64) string { return strconv.FormatInt(n, 10) }
	}
	return &Model{
		ctx:     ctx,
		src:     src,
		opts:    opts,
		stack:   []level{{path: root}},
		cache:   map[string]Listing{},
		loading: map[string]bool{},
		width:   80,
		height:  24,
	}
}

// Run runs the browser until the user quits or ctx is canceled.
func Run(ctx context.Context, src Source, root string, opts Options) error {
	_, err := tea.NewProgram(New(ctx, src, root, opts), tea.WithContext(ctx)).Run()
	return err
}

// Path returns the directory currently being displayed.
func (m *Model) Path() string {
	return m.cur().path
}

// Selected returns the currently selected entry, if any.
func (m *Model) Selected() (Entry, bool) {
	children := m.children()
	if c := m.cur().cursor; c < len(children) {
		return children[c], true
	}
	return Entry{}, false
}

func (m *Model) cur() *level {
	return &m.stack[len(m.stack)-1]
}

func (m *Model) children() []Entry {
	return m.cache[m.cur().path].Children
}

// Init implements tea.Model.
func (m *Model) Init() tea.Cmd {
	return m.load(m.cur().path)
}

// load returns a command that lists path, or nil if it is cached or
// already being loaded.
func (m *Model) load(path string) tea.Cmd {
	if _, ok := m.cache[path]; ok || m.loading[path] {
		return nil
	}
	m.loading[path] = true
	ctx, src := m.ctx, m.src
	return func() tea.Msg {
		l, err := src.List(ctx, path)
		return listingMsg{path: path, listing: l, err: err}
	}
}

// Update implements tea.Model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.move(0)
	case listingMsg:
		delete(m.loading, msg.path)
		if msg.err != nil {
			// Errors for a directory the user has since left are dropped.
			if msg.path == m.cur().path {
				m.err = msg.err
			}
			return m, nil
		}
		m.sortBy.sort(msg.listing.Children)
		m.cache[msg.path] = msg.listing
		m.move(0)
	case tea.KeyPressMsg:
		return m, m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyPressMsg) tea.Cmd {
	switch msg.String() {
	case "q", "ctrl+c":
		return tea.Quit
	case "up", "k":
		m.move(-1)
	case "down", "j":
		m.move(1)
	case "pgup":
		m.move(-m.rows())
	case "pgdown":
		m.move(m.rows())
	case "home", "g":
		m.move(-len(m.children()))
	case "end", "G":
		m.move(len(m.children()))
	case "enter", "right", "l":
		return m.descend()
	case "backspace", "left", "h":
		m.ascend()
	case "s":
		m.sortBy = (m.sortBy + 1) % numSortBy
		for _, l := range m.cache {
			m.sortBy.sort(l.Children)
		}
	case "r":
		delete(m.cache, m.cur().path)
		m.err = nil
		return m.load(m.cur().path)
	}
	return nil
}

func (m *Model) descend() tea.Cmd {
	e, ok := m.Selected()
	if !ok || !e.HasChildren() {
		return nil
	}
	m.stack = append(m.stack, level{path: e.Path})
	m.err = nil
	return m.load(e.Path)
}

func (m *Model) ascend() {
	if len(m.stack) > 1 {
		m.stack = m.stack[:len(m.stack)-1]
		m.err = nil
	}
}

// header and footer lines surround the listing.
const headerLines, footerLines = 2, 1

func (m *Model) rows() int {
	return max(1, m.height-headerLines-footerLines)
}

// move moves the cursor by delta, clamped to the listing, and scrolls
// to keep it visible.
func (m *Model) move(delta int) {
	l, n := m.cur(), len(m.children())
	l.cursor = max(0, min(l.cursor+delta, n-1))
	switch rows := m.rows(); {
	case l.cursor < l.offset:
		l.offset = l.cursor
	case l.cursor >= l.offset+rows:
		l.offset = l.cursor - rows + 1
	}
}

var (
	titleStyle    = lipgloss.NewStyle().Bold(true)
	dimStyle      = lipgloss.NewStyle().Faint(true)
	selectedStyle = lipgloss.NewStyle().Reverse(true)
	errorStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
)

// View implements tea.Model.
func (m *Model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	return v
}

func (m *Model) render() string {
	var b strings.Builder
	cur := m.cur()
	listing, loaded := m.cache[cur.path]
	self := listing.Self

	m.line(&b, titleStyle, fmt.Sprintf("idu  %v", cur.path))
	if loaded {
		m.line(&b, dimStyle, fmt.Sprintf("%v in %v files, %v dirs; %v in this directory  [sort: %v]",
			m.opts.FormatBytes(self.Tree.StorageBytes), m.opts.FormatCount(self.Tree.Files),
			m.opts.FormatCount(self.Dirs), m.opts.FormatBytes(self.Own.StorageBytes), m.sortBy))
	} else {
		m.line(&b, dimStyle, "")
	}

	rows := m.rows()
	children := listing.Children
	end := min(cur.offset+rows, len(children))
	for i := cur.offset; i < end; i++ {
		style := lipgloss.NewStyle()
		if i == cur.cursor {
			style = selectedStyle
		}
		m.line(&b, style, m.row(children[i], self))
	}
	for i := end - cur.offset; i < rows; i++ {
		b.WriteByte('\n')
	}

	switch {
	case m.err != nil:
		b.WriteString(errorStyle.Render(ansi.Truncate("error: "+m.err.Error(), m.width, "…")))
	case m.loading[cur.path]:
		b.WriteString(dimStyle.Render(fmt.Sprintf("loading %v…", cur.path)))
	default:
		b.WriteString(dimStyle.Render(ansi.Truncate("↑↓ move  ⏎/→ open  ←/⌫ up  s sort  r refresh  q quit", m.width, "…")))
	}
	return b.String()
}

// line writes s, truncated to the width of the terminal, using style.
func (m *Model) line(b *strings.Builder, style lipgloss.Style, s string) {
	b.WriteString(style.Render(ansi.Truncate(s, m.width, "…")))
	b.WriteByte('\n')
}

const barWidth = 10

// row formats e, with a bar showing its share of parent.
func (m *Model) row(e, parent Entry) string {
	v, total := m.sortBy.value(e), m.sortBy.value(parent)
	filled := 0
	if total > 0 {
		filled = int((float64(v)/float64(total))*barWidth + 0.5)
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("·", barWidth-filled)
	// The second column shows the number of files, or the storage used when
	// sorting by files, so that the two columns never repeat each other.
	value, other := m.opts.FormatBytes(v), m.opts.FormatCount(e.Tree.Files)
	if m.sortBy == ByFiles {
		value, other = m.opts.FormatCount(v), m.opts.FormatBytes(e.Tree.StorageBytes)
	}
	name := e.Name
	if e.HasChildren() {
		name += "/"
	}
	return fmt.Sprintf("%12s %s %12s  %s", value, bar, other, name)
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
