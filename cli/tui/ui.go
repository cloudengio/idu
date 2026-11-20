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
	"strings"
	"sync"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/cli/tui/internal/model"
	"cloudeng.io/cmd/idu/cli/tui/internal/widgets"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type uiConfig struct {
	env        *cli.Env
	fs         iduconfig.Filesystem
	target     string
	topN       int
	screen     tcell.Screen
	openSource func(context.Context, *cli.Env, iduconfig.Filesystem, io.Writer) (model.Source, error)
	analyze    func(context.Context, analyze.Progress, io.Writer, *analyze.Flags) error
}

// The names of the pages displayed beneath the menu bar.
const (
	viewPage    = "view"
	analyzePage = "analyze"
)

// Limits on the proportion of the screen used by the top-N pane.
const (
	minBottomWeight = 1
	maxBottomWeight = 8
	topWeight       = 4
)

type ui struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    uiConfig
	app    *tview.Application
	// pages holds the body, ie. everything but the menu bar and status
	// line, and the menu bar's drop-down menus.
	pages   *tview.Pages
	body    *tview.Pages
	menu    *widgets.MenuBar
	status  *tview.TextView
	view    *viewScreen
	analyze *analyzeScreen

	// statuses are the messages to be displayed in the status line, see
	// setStatus.
	statuses chan string

	mu  sync.Mutex
	src model.Source
	// analyzing is true while an analysis is running, analysisDone is
	// closed when the most recent analysis has finished.
	analyzing    bool
	analysisDone chan struct{}
	// quitting is true once quitting has been requested and the tui is
	// waiting for an analysis to finish.
	quitting bool
}

func newUI(ctx context.Context, cfg uiConfig) *ui {
	ctx, cancel := context.WithCancel(ctx)
	// Messages written while waiting for the database to open would
	// otherwise be written to the terminal.
	env := *cfg.env
	cfg.env = &env
	u := &ui{ctx: ctx, cancel: cancel, cfg: cfg, app: tview.NewApplication().SetScreen(cfg.screen).EnableMouse(true), statuses: make(chan string, 100)}
	env.OpenProgress = statusWriter{u}
	u.pages = tview.NewPages()
	u.body = tview.NewPages()
	u.status = tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	u.menu = widgets.NewMenuBar(u.app, u.pages)
	u.view = newViewScreen(u)
	u.analyze = newAnalyzeScreen(u)
	u.body.AddPage(viewPage, u.view.layout, true, true)
	u.body.AddPage(analyzePage, u.analyze.layout, true, false)
	u.setupMenu()
	main := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(u.menu, 1, 0, false).
		AddItem(u.body, 0, 1, true).
		AddItem(u.status, 1, 0, false)
	u.pages.AddPage("main", main, true, true)
	u.app.SetRoot(u.pages, true).SetFocus(u.view.tree)
	u.app.SetInputCapture(u.input)
	return u
}

func (u *ui) setupMenu() {
	view := []widgets.MenuItem{
		{Label: "Home", Action: func() { u.showView(); u.view.home() }},
		{Label: "Refresh", Action: u.refresh},
		{Label: "Hotspots", Action: func() { u.showView(); u.view.tabs.Select(hotspotsTab) }},
	}
	u.menu.AddMenu("View", view...).AddAction("Analyze", func() { u.startAnalyze(false) }).AddAction("Quit", u.quit).
		SetRight(fmt.Sprintf("idu: %v   [::d]/ m: menu  ←/→ tab: panes  [ ]: tabs  +/-/z: resize  q: quit", iduconfig.Label(u.cfg.fs)))
}

// run starts the tui and returns when it is quit.
func (u *ui) run() error {
	defer u.cancel()
	go u.displayStatuses()
	go u.start()
	err := u.app.Run()
	u.closeSource()
	return err
}

// start opens the database, running an analysis first if the filesystem
// has never been analyzed.
func (u *ui) start() {
	cfg := u.cfg.fs.Config()
	if _, err := os.Stat(cfg.Database); errors.Is(err, os.ErrNotExist) {
		u.setStatus("the filesystem has not been analyzed, running analyze")
		u.app.QueueUpdateDraw(func() { u.startAnalyze(false) })
		return
	}
	u.setStatus("opening the database (waiting if it is locked by another process)")
	if err := u.openSource(); err != nil {
		if errors.Is(err, model.ErrNotAnalyzed) {
			u.setStatus("the filesystem has not been analyzed, running analyze")
			u.app.QueueUpdateDraw(func() { u.startAnalyze(false) })
			return
		}
		u.app.QueueUpdateDraw(func() { u.openFailed(err) })
		return
	}
	u.app.QueueUpdateDraw(func() { u.view.load(u.cfg.target) })
}

// openFailed reports that the database could not be opened and offers
// the appropriate remedy. It must be called on the tui's goroutine.
func (u *ui) openFailed(err error) {
	u.status.SetText("")
	if errors.Is(err, idudb.ErrStaleSummaries) {
		u.showDialog(errorDialog, fmt.Sprintf("The summaries for %v cannot be used:\n\n%v\n\nAnalyze the filesystem now?", iduconfig.Label(u.cfg.fs), err),
			dialogButton{"Analyze", func() { u.startAnalyze(false) }},
			dialogButton{"Quit", u.quit})
		return
	}
	u.showDialog(errorDialog, fmt.Sprintf("Failed to open the database for %v:\n\n%v", iduconfig.Label(u.cfg.fs), err),
		dialogButton{"Retry", func() { go u.start() }},
		dialogButton{"Quit", u.quit})
}

// dialogPage is the name of the page used to display dialogs.
const dialogPage = "dialog"

type dialogButton struct {
	label  string
	action func()
}

// The kinds of dialog.
const (
	errorDialog   = "Error"
	warningDialog = "Warning"
)

// showDialog displays message in a dialog, of the specified kind, in the
// centre of the screen with the supplied buttons, each of which closes the
// dialog before running its action. A dialog without buttons has an OK
// button. It must be called on the tui's goroutine.
func (u *ui) showDialog(kind, message string, buttons ...dialogButton) {
	if len(buttons) == 0 {
		buttons = []dialogButton{{"OK", nil}}
	}
	labels := make([]string, len(buttons))
	for i, b := range buttons {
		labels[i] = b.label
	}
	focus := u.app.GetFocus()
	modal := tview.NewModal().SetText(message).AddButtons(labels).
		SetDoneFunc(func(i int, _ string) {
			u.pages.RemovePage(dialogPage)
			u.app.SetFocus(focus)
			if i >= 0 && i < len(buttons) && buttons[i].action != nil {
				buttons[i].action()
			}
		})
	color := tcell.ColorRed
	if kind == warningDialog {
		color = tcell.ColorYellow
	}
	modal.SetBorderColor(color).SetTitle(" " + kind + " ")
	u.pages.RemovePage(dialogPage)
	u.pages.AddPage(dialogPage, modal, true, true)
	u.app.SetFocus(modal)
}

// showError displays err in a dialog, it may be called from any
// goroutine, including the tui's, see displayStatuses.
func (u *ui) showError(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	go u.app.QueueUpdateDraw(func() { u.showDialog(errorDialog, msg) })
}

// dialogOpen returns true if a dialog is displayed.
func (u *ui) dialogOpen() bool {
	return u.pages.HasPage(dialogPage)
}

func (u *ui) openSource() error {
	src, err := u.cfg.openSource(u.ctx, u.cfg.env, u.cfg.fs, statusWriter{u})
	if err != nil {
		return err
	}
	u.mu.Lock()
	u.src = src
	u.mu.Unlock()
	u.setStatus("database opened")
	if w, fixable := src.Warning(); w != "" {
		u.app.QueueUpdateDraw(func() {
			if !fixable {
				u.showDialog(warningDialog, w+".", dialogButton{"Continue", nil})
				return
			}
			u.showDialog(warningDialog, w+".\n\nAnalyze the filesystem now to update them?",
				dialogButton{"Continue", nil},
				dialogButton{"Analyze", func() { u.startAnalyze(false) }})
		})
	}
	return nil
}

func (u *ui) source() model.Source {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.src
}

func (u *ui) closeSource() {
	u.mu.Lock()
	src := u.src
	u.src = nil
	u.mu.Unlock()
	if src != nil {
		_ = src.Close(context.WithoutCancel(u.ctx))
	}
}

// refresh reopens the database and reloads the view.
func (u *ui) refresh() {
	if u.isAnalyzing() {
		return
	}
	u.showView()
	u.view.reset()
	u.closeSource()
	go func() {
		if err := u.openSource(); err != nil {
			u.app.QueueUpdateDraw(func() { u.openFailed(err) })
			return
		}
		u.app.QueueUpdateDraw(func() { u.view.load(u.view.currentPath()) })
	}()
}

// quit exits the tui. If an analysis is running it first asks whether to
// cancel it and, if so, waits for it to finish cleanly, as it does when
// interrupted, before exiting. A request to quit while waiting exits
// immediately. It must be called on the tui's goroutine.
func (u *ui) quit() {
	u.mu.Lock()
	analyzing, quitting := u.analyzing, u.quitting
	u.mu.Unlock()
	if !analyzing || quitting {
		u.stop()
		return
	}
	u.showDialog(warningDialog, "An analysis is running. Cancel it and quit once it has finished cleanly?",
		dialogButton{"Cancel analysis and quit", u.quitAfterAnalysis},
		dialogButton{"Continue", nil})
}

// quitAfterAnalysis cancels the running analysis and exits once it has
// finished. It must be called on the tui's goroutine.
func (u *ui) quitAfterAnalysis() {
	u.mu.Lock()
	u.quitting = true
	done := u.analysisDone
	u.mu.Unlock()
	if done == nil {
		u.stop()
		return
	}
	u.analyze.cancelAnalysis()
	u.status.SetText("canceling the analysis, waiting for all filesystem and database operations to finish (q to quit immediately)")
	go func() {
		<-done
		u.app.QueueUpdate(u.stop)
	}()
}

// isQuitting returns true if the tui is waiting to quit.
func (u *ui) isQuitting() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.quitting
}

// stop exits the tui immediately.
func (u *ui) stop() {
	u.cancel()
	u.app.Stop()
}

func (u *ui) isAnalyzing() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.analyzing
}

func (u *ui) showView() {
	u.body.SwitchToPage(viewPage)
	u.app.SetFocus(u.view.tree)
}

// setStatus displays a message in the status line, it may be called from
// any goroutine, including the tui's, and never blocks; messages are
// displayed in order and discarded if they cannot be displayed quickly
// enough.
func (u *ui) setStatus(format string, args ...any) {
	select {
	case u.statuses <- fmt.Sprintf(format, args...):
	default:
	}
}

// displayStatuses displays the messages queued by setStatus. QueueUpdate
// waits for the update to be run on the tui's goroutine and hence must not
// be called from that goroutine, which is why the messages are displayed
// by this goroutine.
func (u *ui) displayStatuses() {
	for {
		select {
		case msg := <-u.statuses:
			u.app.QueueUpdateDraw(func() { u.status.SetText(msg) })
		case <-u.ctx.Done():
			return
		}
	}
}

// statusWriter displays whatever is written to it in the status line.
type statusWriter struct{ u *ui }

func (w statusWriter) Write(p []byte) (int, error) {
	w.u.setStatus("%s", trimNewline(string(p)))
	return len(p), nil
}

func trimNewline(s string) string {
	return strings.TrimSpace(s)
}

func (u *ui) input(ev *tcell.EventKey) *tcell.EventKey {
	if u.dialogOpen() || u.menu.IsOpen() || u.app.GetFocus() == u.menu {
		if ev.Key() == tcell.KeyCtrlC {
			u.quit()
			return nil
		}
		return ev
	}
	switch ev.Key() {
	case tcell.KeyF10:
		u.menu.Activate()
		return nil
	case tcell.KeyCtrlC:
		u.quit()
		return nil
	case tcell.KeyRune:
		if u.runeInput(ev) {
			return nil
		}
	}
	if name, _ := u.body.GetFrontPage(); name == viewPage {
		return u.view.input(ev)
	}
	return u.analyze.input(ev)
}

// runeInput handles the keys that apply to every screen and returns true
// if ev was one of them.
func (u *ui) runeInput(ev *tcell.EventKey) bool {
	switch {
	case ev.Rune() == 'q':
		u.quit()
	case ev.Rune() == '/', ev.Rune() == 'm' && ev.Modifiers()&tcell.ModAlt != 0:
		u.menu.Activate()
	case ev.Rune() == 'm':
		// In the top-N pane m selects its tabs, and in the tab bar the
		// main menu.
		if name, _ := u.body.GetFrontPage(); name == viewPage && u.view.inBottomPane() && !u.view.tabs.IsActive() {
			u.view.tabs.Activate()
		} else {
			u.menu.Activate()
		}
	default:
		return false
	}
	return true
}
