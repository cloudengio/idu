// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type harness struct {
	t      *testing.T
	u      *ui
	screen tcell.SimulationScreen
	done   chan error
	root   string
}

// start runs the tui, for a hardlink tree that has been analyzed if
// analyzed is true, on a simulation screen.
func start(t *testing.T, analyzed bool) *harness {
	t.Helper()
	if !analyzed {
		return startWith(t, nil)
	}
	return startWith(t, func(tmpDir, root string) iduconfig.T {
		return clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	})
}

// startWith runs the tui, on a simulation screen, for a hardlink tree
// analyzed with the configuration returned by analyzeWith, if not nil,
// using the default configuration.
func startWith(t *testing.T, analyzeWith func(tmpDir, root string) iduconfig.T) *harness {
	t.Helper()
	return startHarness(t, analyzeWith, nil)
}

// startHarness is like startWith, but uses analyzer, if not nil, for the
// analyses run by the tui.
func startHarness(t *testing.T, analyzeWith func(tmpDir, root string) iduconfig.T,
	analyzer func(context.Context, analyze.Progress, io.Writer, *analyze.Flags) error) *harness {
	t.Helper()
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)
	cfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	env := cli.NewEnv()
	env.OpenProgress = io.Discard
	env.Config = cfg
	f := cfg.Filesystems[0]
	c := New(env)
	if analyzer == nil {
		analyzer = c.analyzer(env, f)
	}
	if analyzeWith != nil {
		acfg := analyzeWith(tmpDir, root)
		aenv := *env
		aenv.Config = acfg
		if err := c.analyzer(&aenv, acfg.Filesystems[0])(ctx, nil, io.Discard, &analyze.Flags{}); err != nil {
			t.Fatal(err)
		}
	}
	screen := tcell.NewSimulationScreen("UTF-8")
	u := newUI(ctx, uiConfig{
		env:        env,
		fs:         f,
		target:     root,
		topN:       10,
		screen:     screen,
		openSource: c.sourceOpener(),
		analyze:    analyzer,
	})
	screen.SetSize(140, 45)
	h := &harness{t: t, u: u, screen: screen, done: make(chan error, 1), root: root}
	go func() { h.done <- u.run() }()
	t.Cleanup(func() {
		// QueueUpdate waits for the update to run, which it never does
		// once the tui has exited.
		go u.app.QueueUpdate(u.stop)
		select {
		case err := <-h.done:
			if err != nil {
				t.Errorf("tui: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("the tui did not exit")
		}
	})
	return h
}

// text returns the contents of the screen. The screen is read on the tui's
// goroutine since it is written whenever the tui draws.
func (h *harness) text() string {
	var text string
	done := make(chan struct{})
	h.u.app.QueueUpdate(func() { text = h.contents(); close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		return "<the tui is not responding>"
	}
	return text
}

func (h *harness) contents() string {
	cells, width, _ := h.screen.GetContents()
	var b strings.Builder
	for i, c := range cells {
		if len(c.Runes) > 0 {
			b.WriteRune(c.Runes[0])
		} else {
			b.WriteRune(' ')
		}
		if (i+1)%width == 0 {
			b.WriteRune('\n')
		}
	}
	return b.String()
}

// waitFor waits until the screen contains all of want.
func (h *harness) waitFor(want ...string) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		text := h.text()
		missing := ""
		for _, w := range want {
			if !strings.Contains(text, w) {
				missing = w
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %q:\n%s", missing, text)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *harness) key(k tcell.Key) {
	h.screen.InjectKey(k, 0, tcell.ModNone)
}

func (h *harness) rune(r rune) {
	h.screen.InjectKey(tcell.KeyRune, r, tcell.ModNone)
}

// inUI runs fn on the tui's goroutine and waits for it to complete.
func (h *harness) inUI(fn func()) {
	done := make(chan struct{})
	h.u.app.QueueUpdateDraw(func() { fn(); close(done) })
	<-done
}

// eventually waits until cond, which is run on the tui's goroutine,
// returns true.
func (h *harness) eventually(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ok := false
		h.inUI(func() { ok = cond() })
		if ok {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %v:\n%s", what, h.text())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestView(t *testing.T) {
	h := start(t, true)
	// The root's children are displayed, largest first, and the root's
	// summary and top-N rankings.
	h.waitFor("+ b ", "  a-b ", "Subtree", "Top-N beneath "+h.root+" ", filepath.Join(h.root, "ab"))
	text := h.text()
	if strings.Index(text, "  a ") > strings.Index(text, "  a-b ") {
		t.Errorf("children are not sorted by size:\n%s", text)
	}
	// Each prefix is labeled with the storage used by, and the number of
	// files in, its subtree; b contains h and c/l3, which is a hardlink to
	// a/f2 and hence is not counted.
	var labels []string
	h.inUI(func() {
		for _, c := range h.u.view.tree.GetRoot().GetChildren() {
			labels = append(labels, c.GetText())
		}
	})
	label := regexp.MustCompile(`^\+ b  [0-9.]+ [kKMGT]?B \(1\)$`)
	if !slices.ContainsFunc(labels, label.MatchString) {
		t.Errorf("no label for b matching %v: %q", label, labels)
	}
	// Moving the highlight displays the summary of the highlighted prefix.
	h.key(tcell.KeyDown)
	h.eventually("a to be highlighted", func() bool { return h.u.view.currentPath() == filepath.Join(h.root, "a") })
	h.waitFor("files      2")
	// Expanding b displays its child c; b may be one or two rows below a
	// depending on whether directories themselves allocate storage blocks.
	bPath := filepath.Join(h.root, "b")
	for range 3 {
		var cur string
		h.inUI(func() { cur = h.u.view.currentPath() })
		if cur == bPath {
			break
		}
		h.key(tcell.KeyDown)
		h.eventually("highlight to move down", func() bool { return h.u.view.currentPath() != cur })
	}
	h.eventually("b to be highlighted", func() bool { return h.u.view.currentPath() == bPath })
	h.key(tcell.KeyEnter)
	h.waitFor("  c ")
	// The rankings beneath b are computed from its summaries.
	h.waitFor("Top-N beneath " + filepath.Join(h.root, "b") + " ")
	// Selecting a row in the top-N table highlights its prefix.
	h.key(tcell.KeyTab)
	h.key(tcell.KeyTab)
	h.key(tcell.KeyEnter)
	h.eventually("b/c to be highlighted", func() bool { return h.u.view.currentPath() == filepath.Join(h.root, "b", "c") })
}

func TestTabsAndResize(t *testing.T) {
	h := start(t, true)
	h.waitFor("Top-N beneath " + h.root + " ")
	h.rune(']')
	h.rune('+')
	h.eventually("storage tab, larger pane", func() bool {
		return h.u.view.bottomWeight == 4 && h.u.view.tabs.Current() == storageTab
	})
	for range maxBottomWeight + 2 {
		h.rune('+')
	}
	h.rune('[')
	h.eventually("bytes tab, maximum pane", func() bool {
		return h.u.view.bottomWeight == maxBottomWeight && h.u.view.tabs.Current() == bytesTab
	})
	for range maxBottomWeight + 2 {
		h.rune('-')
	}
	h.eventually("minimum pane", func() bool { return h.u.view.bottomWeight == minBottomWeight })
	h.rune('z')
	h.eventually("maximized", func() bool { return h.u.view.maximized })
	h.rune('z')
	h.eventually("restored", func() bool { return !h.u.view.maximized })
	// The hotspots are not included in the stored state.
	h.inUI(func() { h.u.view.tabs.Select(hotspotsTab) })
	h.waitFor("The hotspots are not included in these stats")
}

func TestMenu(t *testing.T) {
	h := start(t, true)
	h.waitFor("Top-N beneath " + h.root + " ")
	h.key(tcell.KeyF10)
	h.key(tcell.KeyEnter)
	h.waitFor("Refresh", "Hotspots")
	h.key(tcell.KeyEscape)
	h.eventually("the menu to close", func() bool { return !h.u.menu.IsOpen() })
	// Analyze is a menu bar item that starts an analysis.
	h.key(tcell.KeyF10)
	h.key(tcell.KeyRight)
	h.key(tcell.KeyEnter)
	h.eventually("the analysis to start", func() bool { return h.u.isAnalyzing() || h.u.source() == nil })
	h.eventually("the analysis to complete and the view to reload", func() bool {
		name, _ := h.u.body.GetFrontPage()
		return !h.u.isAnalyzing() && name == viewPage && h.u.source() != nil
	})
	h.waitFor("+ b ", "Top-N beneath "+h.root+" ")
}

func TestAnalyzeWhenNotAnalyzed(t *testing.T) {
	h := start(t, false)
	// The analysis is run automatically, and then the view displayed.
	h.waitFor("+ b ", "Top-N beneath "+h.root+" ")
}

func TestQuit(t *testing.T) {
	h := start(t, true)
	h.waitFor("+ b ")
	h.rune('q')
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.done <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("q did not quit")
	}
}

// bytesConfig returns a configuration for root that uses a different
// calculator, bytes, to the default, allocated.
func bytesConfig(t *testing.T, tmpDir, root string) iduconfig.T {
	t.Helper()
	cfg, err := iduconfig.ParseConfig([]byte("filesystems:\n  - root: " + root +
		"\n    database: " + filepath.Join(tmpDir, "db") +
		"\n    layout:\n      calculator: bytes\n"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestIncompatibleSummaries(t *testing.T) {
	h := startWith(t, func(tmpDir, root string) iduconfig.T { return bytesConfig(t, tmpDir, root) })
	// The error is displayed in a dialog that offers to analyze.
	h.waitFor("Error", "cannot be used", "calculator", "Analyze", "Quit")
	h.eventually("the dialog", func() bool { return h.u.dialogOpen() })
	// Keys other than those for the dialog are ignored.
	h.rune('m')
	h.key(tcell.KeyEnter)
	h.eventually("the analysis to complete and the view to reload", func() bool {
		return !h.u.dialogOpen() && !h.u.isAnalyzing() && h.u.source() != nil
	})
	h.waitFor("+ b ", "Top-N beneath "+h.root+" ")
}

func TestSummariesWithoutHardlinks(t *testing.T) {
	h := startWith(t, func(tmpDir, root string) iduconfig.T {
		return clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupRescan)
	})
	// The summaries are used, with a warning.
	h.waitFor("Warning", "counted more than once", "Continue", "Analyze")
	h.key(tcell.KeyEnter)
	h.eventually("the dialog to close", func() bool { return !h.u.dialogOpen() })
	h.waitFor("+ b ", "Top-N beneath "+h.root+" ", filepath.Join(h.root, "ab"))
}

func TestMenuKeysAndMouse(t *testing.T) {
	h := start(t, true)
	h.waitFor("Top-N beneath " + h.root + " ")
	h.rune('m')
	h.key(tcell.KeyEnter)
	h.waitFor("Refresh", "Hotspots", "Quit")
	h.key(tcell.KeyEscape)
	h.eventually("the menu to close", func() bool { return !h.u.menu.IsOpen() })
	// Clicking a menu title opens it, View is the first.
	h.screen.InjectMouse(2, 0, tcell.Button1, tcell.ModNone)
	h.screen.InjectMouse(2, 0, tcell.ButtonNone, tcell.ModNone)
	h.waitFor("Refresh", "Hotspots")
}

func TestQuitMenu(t *testing.T) {
	h := start(t, true)
	h.waitFor("+ b ", " Quit ")
	// Quit is the third item in the menu bar and exits immediately.
	h.rune('m')
	h.key(tcell.KeyRight)
	h.key(tcell.KeyRight)
	h.key(tcell.KeyEnter)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.done <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("Quit did not exit")
	}
}

// slowAnalysis is an analysis that runs until canceled and then takes
// finishDelay to finish.
type slowAnalysis struct {
	finishDelay time.Duration
	mu          sync.Mutex
	finished    bool
}

func (s *slowAnalysis) analyze(ctx context.Context, _ analyze.Progress, _ io.Writer, _ *analyze.Flags) error {
	<-ctx.Done()
	time.Sleep(s.finishDelay)
	s.mu.Lock()
	s.finished = true
	s.mu.Unlock()
	return ctx.Err()
}

func (s *slowAnalysis) isFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

func (h *harness) exited() bool {
	select {
	case err := <-h.done:
		h.done <- err
		return true
	case <-time.After(50 * time.Millisecond):
		return false
	}
}

func TestQuitDuringAnalysis(t *testing.T) {
	slow := &slowAnalysis{finishDelay: 300 * time.Millisecond}
	// Without a database the tui starts an analysis immediately.
	h := startHarness(t, nil, slow.analyze)
	h.eventually("the analysis to start", h.u.isAnalyzing)
	// Quitting asks for confirmation, Continue leaves the analysis running.
	h.rune('q')
	h.waitFor("An analysis is running", "Cancel analysis and quit", "Continue")
	h.key(tcell.KeyTab)
	h.key(tcell.KeyEnter)
	h.eventually("the dialog to close", func() bool { return !h.u.dialogOpen() })
	if !h.u.isAnalyzing() || h.exited() {
		t.Fatalf("the analysis was not left running")
	}
	// Confirming cancels the analysis and exits once it has finished.
	h.rune('q')
	h.waitFor("An analysis is running")
	h.key(tcell.KeyEnter)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.done <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("quit did not exit")
	}
	if !slow.isFinished() {
		t.Errorf("the tui exited before the analysis finished")
	}
}

func TestForceQuitDuringAnalysis(t *testing.T) {
	slow := &slowAnalysis{finishDelay: time.Minute}
	h := startHarness(t, nil, slow.analyze)
	h.eventually("the analysis to start", h.u.isAnalyzing)
	h.rune('q')
	h.waitFor("An analysis is running")
	h.key(tcell.KeyEnter)
	h.eventually("waiting to quit", h.u.isQuitting)
	if h.exited() {
		t.Fatalf("exited before the analysis finished")
	}
	// A second request to quit exits immediately.
	h.rune('q')
	select {
	case err := <-h.done:
		h.done <- err
	case <-time.After(10 * time.Second):
		t.Fatal("q did not exit immediately")
	}
	if slow.isFinished() {
		t.Errorf("the analysis should still be running")
	}
}

func TestBottomPaneMenu(t *testing.T) {
	h := start(t, true)
	h.waitFor("Top-N beneath " + h.root + " ")
	// In the top-N pane m selects its tabs.
	h.key(tcell.KeyTab)
	h.key(tcell.KeyTab)
	h.eventually("the top-N pane to have the focus", h.u.view.inBottomPane)
	h.rune('m')
	h.eventually("the tab bar to be active", h.u.view.tabs.IsActive)
	h.waitFor("left/right: select")
	h.key(tcell.KeyRight)
	h.key(tcell.KeyRight)
	h.eventually("the files tab", func() bool { return h.u.view.tabs.Current() == filesTab })
	h.key(tcell.KeyEnter)
	h.eventually("the files table to have the focus", func() bool {
		return !h.u.view.tabs.IsActive() && h.u.app.GetFocus() == h.u.view.tables[filesTab]
	})
	// In the tab bar m selects the main menu.
	h.rune('m')
	h.rune('m')
	h.eventually("the main menu", func() bool { return h.u.app.GetFocus() == h.u.menu })
}

func TestAnalyzeScreen(t *testing.T) {
	slow := &slowAnalysis{finishDelay: 0}
	h := startHarness(t, func(tmpDir, root string) iduconfig.T {
		return clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	}, slow.analyze)
	h.waitFor("Top-N beneath " + h.root + " ")
	// Selecting Analyze displays the analyze screen and starts an analysis.
	h.rune('m')
	h.key(tcell.KeyRight)
	h.key(tcell.KeyEnter)
	h.eventually("the analysis to start", h.u.isAnalyzing)
	h.waitFor("running", "analyze for", "previous analysis", "Prefixes completed per second",
		"Files completed per second", "Stats completed per second", "Outstanding operations", "Events",
		"Prefixes scanned, relative to the previous analysis")
	// The previous analysis found 6 prefixes and 4 files, see
	// clitest.CreateHardlinkTree.
	var prev previousTotals
	h.inUI(func() { h.u.analyze.mu.Lock(); prev = h.u.analyze.previous; h.u.analyze.mu.Unlock() })
	if !prev.known || prev.prefixes != 6 || prev.files != 4 || prev.when.IsZero() {
		t.Errorf("unexpected previous totals: %+v", prev)
	}
	// Escape cancels the analysis, which then reloads the view.
	h.key(tcell.KeyEscape)
	h.eventually("the analysis to finish", func() bool { return !h.u.isAnalyzing() && h.u.source() != nil })
}

func TestFormatSample(t *testing.T) {
	s := sample{elapsed: 90 * time.Second}
	s.PrefixesFinished, s.Files, s.FSStats, s.Errors = 3, 1234, 4000, 2
	out := formatSample(s, previousTotals{known: true, prefixes: 6, files: 4}, "analyze --snapshot")
	for _, want := range []string{"running", "analyze --snapshot for 1m30s", "1,234", "4,000", "errors 2"} {
		if !strings.Contains(out, want) {
			t.Errorf("%q does not contain %q", out, want)
		}
	}
	if !strings.Contains(regexp.MustCompile(`\s+`).ReplaceAllString(out, " "), "prefixes 3 6") {
		t.Errorf("missing prefixes: %q", out)
	}
	s.done = true
	if out := formatSample(s, previousTotals{}, "analyze"); !strings.Contains(out, "completed") || !strings.Contains(out, "unknown") {
		t.Errorf("unexpected: %q", out)
	}
}

// labelStyle returns the style used to draw the first cell of the first
// line on which text appears.
func (h *harness) labelStyle(text string) (tcell.Style, bool) {
	var style tcell.Style
	found := false
	done := make(chan struct{})
	h.u.app.QueueUpdate(func() {
		defer close(done)
		cells, width, height := h.screen.GetContents()
		for y := range height {
			var line strings.Builder
			for x := range width {
				c := cells[y*width+x]
				if len(c.Runes) > 0 {
					line.WriteRune(c.Runes[0])
				} else {
					line.WriteRune(' ')
				}
			}
			if i := strings.Index(line.String(), text); i >= 0 {
				style, found = cells[y*width+i].Style, true
				return
			}
		}
	})
	<-done
	return style, found
}

func TestLeafLabelsVisible(t *testing.T) {
	h := start(t, true)
	// a-b has no child prefixes, b does.
	h.waitFor("  a-b ", "+ b ")
	leaf, ok := h.labelStyle("  a-b ")
	if !ok {
		t.Fatalf("no label for a-b:\n%s", h.text())
	}
	parent, _ := h.labelStyle("+ b ")
	lfg, _, _ := leaf.Decompose()
	pfg, _, _ := parent.Decompose()
	// Prefixes without children must be drawn in the same, visible, color
	// as those with children; tcell.ColorGray may be the same as the
	// terminal's background.
	if lfg != pfg || lfg == tcell.ColorGray {
		t.Errorf("leaf label color %v, parent label color %v", lfg, pfg)
	}
}

func TestArrowKeys(t *testing.T) {
	h := start(t, true)
	h.waitFor("+ b ", "Top-N beneath "+h.root+" ")
	focused := func(what string, p func() tview.Primitive) {
		t.Helper()
		h.eventually(what, func() bool { return h.u.app.GetFocus() == p() })
	}
	tree := func() tview.Primitive { return h.u.view.tree }
	summary := func() tview.Primitive { return h.u.view.summary }
	table := func() tview.Primitive { return h.u.view.tables[h.u.view.tabs.Current()] }
	menu := func() tview.Primitive { return h.u.menu }

	// / selects the menu bar, escape returns to the tree.
	h.rune('/')
	focused("the menu bar", menu)
	h.key(tcell.KeyEscape)
	focused("the tree", tree)
	// Up moves up the tree, and at its root selects the menu bar.
	h.key(tcell.KeyDown)
	h.eventually("a to be highlighted", func() bool { return h.u.view.currentPath() == filepath.Join(h.root, "a") })
	h.key(tcell.KeyUp)
	h.eventually("the root to be highlighted", func() bool { return h.u.view.currentPath() == h.root })
	focused("the tree", tree)
	h.key(tcell.KeyUp)
	focused("the menu bar", menu)
	h.key(tcell.KeyEscape)
	focused("the tree", tree)
	// Left and right move between the panes.
	h.key(tcell.KeyRight)
	focused("the summary", summary)
	h.key(tcell.KeyRight)
	focused("the top-N table", table)
	h.key(tcell.KeyRight)
	focused("the tree", tree)
	h.key(tcell.KeyLeft)
	focused("the top-N table", table)
	// Up at the top of the table selects the tab bar, where left and right
	// select tabs, and then the menu bar.
	h.key(tcell.KeyUp)
	h.eventually("the tab bar", h.u.view.tabs.IsActive)
	h.key(tcell.KeyRight)
	h.eventually("the storage tab", func() bool { return h.u.view.tabs.Current() == storageTab })
	h.key(tcell.KeyUp)
	focused("the menu bar", menu)
}

func TestBracketPathsEscaped(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "tree[blue]")
	sub := filepath.Join(root, "dir[red]sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := iduconfig.ParseConfig([]byte(fmt.Sprintf("filesystems:\n  - root: %s\n    database: %s\n", root, filepath.Join(tmpDir, "db"))))
	if err != nil {
		t.Fatal(err)
	}
	env := cli.NewEnv()
	env.OpenProgress = io.Discard
	env.Config = cfg
	f := cfg.Filesystems[0]
	c := New(env)
	if err := c.analyzer(env, f)(ctx, nil, io.Discard, &analyze.Flags{}); err != nil {
		t.Fatal(err)
	}
	screen := tcell.NewSimulationScreen("UTF-8")
	u := newUI(ctx, uiConfig{
		env:        env,
		fs:         f,
		target:     root,
		topN:       10,
		screen:     screen,
		openSource: c.sourceOpener(),
		analyze:    c.analyzer(env, f),
	})
	screen.SetSize(140, 45)
	h := &harness{t: t, u: u, screen: screen, done: make(chan error, 1), root: root}
	go func() { h.done <- u.run() }()
	t.Cleanup(func() {
		go u.app.QueueUpdate(u.stop)
		select {
		case err := <-h.done:
			if err != nil {
				t.Errorf("tui: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("the tui did not exit")
		}
	})
	h.waitFor("dir[red]sub", "Top-N beneath "+root+" ")
}

func TestTUIProgressStopPreventsStaleRunningRender(t *testing.T) {
	h := start(t, true)
	h.waitFor("Top-N beneath " + h.root + " ")
	p := newTUIProgress(h.u, "analyze")
	p.PrefixDone(0, 5)
	p.Report(context.Background())
	if _, ok := p.sampleIfRunning(); ok {
		t.Errorf("sampleIfRunning returned true after Report/Stop")
	}
	if !p.isStopped() {
		t.Errorf("expected stopped to be true after Report/Stop")
	}
	h.eventually("completed state rendered", func() bool {
		return strings.Contains(h.u.analyze.info.GetText(true), "completed")
	})
}

func TestAnalyzeErrorPreservesErrorDialog(t *testing.T) {
	h := startHarness(t, func(tmpDir, root string) iduconfig.T {
		return clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupRescan)
	}, func(context.Context, analyze.Progress, io.Writer, *analyze.Flags) error {
		return errors.New("boom")
	})
	// Dismiss initial hardlink warning dialog from openSource.
	h.waitFor("Warning", "counted more than once", "Continue")
	h.key(tcell.KeyEnter)
	h.eventually("warning dialog closed", func() bool { return !h.u.dialogOpen() })

	// Trigger Analyze; failure dialog must stay visible rather than being
	// overwritten by openSource's warning dialog.
	h.rune('m')
	h.key(tcell.KeyRight)
	h.key(tcell.KeyEnter)
	h.waitFor("Error", "analyze failed", "boom")
	if strings.Contains(h.text(), "counted more than once") {
		t.Errorf("warning dialog overwrote analyze error:\n%s", h.text())
	}
}
