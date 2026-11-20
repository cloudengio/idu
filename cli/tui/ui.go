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
	errors  *errorsScreen

	// statuses are the messages to be displayed in the status line, see
	// setStatus.
	statuses chan string
	// waitModal is the dialog displayed while waiting for a database to
	// open, and waitFocus the primitive that had the focus before it was
	// displayed; both are only accessed on the tui's goroutine.
	waitModal *tview.Modal
	waitFocus tview.Primitive

	mu  sync.Mutex
	src *sourceHandle
	// analyzing is true while an analysis is running, analysisDone is
	// closed when the most recent analysis has finished.
	analyzing    bool
	analysisDone chan struct{}
	// quitting is true once quitting has been requested and the tui is
	// waiting for an analysis to finish.
	quitting bool
}

// sourceHandle wraps an open model.Source so active view readers can be
// canceled and drained before the underlying database is closed.
type sourceHandle struct {
	src    model.Source
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (h *sourceHandle) release() {
	h.wg.Done()
}

func (h *sourceHandle) close(ctx context.Context) {
	if h == nil {
		return
	}
	h.cancel()
	h.wg.Wait()
	_ = h.src.Close(context.WithoutCancel(ctx))
}

func newUI(ctx context.Context, cfg uiConfig) *ui {
	ctx, cancel := context.WithCancel(ctx)
	u := &ui{ctx: ctx, cancel: cancel, cfg: cfg, app: tview.NewApplication().SetScreen(cfg.screen).EnableMouse(true), statuses: make(chan string, 100)}
	// Messages written while waiting for a database to open, both by the
	// tui and by the analyses that it runs, which share cfg.env, are
	// displayed in a dialog rather than written to the terminal.
	cfg.env.OpenProgress = openWaitWriter{u}
	u.pages = tview.NewPages()
	u.body = tview.NewPages()
	u.status = tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	u.menu = widgets.NewMenuBar(u.app, u.pages)
	u.view = newViewScreen(u)
	u.analyze = newAnalyzeScreen(u)
	u.errors = newErrorsScreen(u)
	u.body.AddPage(viewPage, u.view.layout, true, true)
	u.body.AddPage(analyzePage, u.analyze.layout, true, false)
	u.body.AddPage(errorsPage, u.errors.layout, true, false)
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
		{Label: "Errors", Action: u.showErrors},
	}
	u.menu.AddMenu("View", view...).AddAction("Analyze", func() { u.startAnalyze(false) }).AddAction("Quit", u.quit).
		SetRight(fmt.Sprintf("idu: %v   [::d]/ m: menu  ←/→ tab: panes  [ ]: tabs  +/-/z: resize  q: quit", tview.Escape(iduconfig.Label(u.cfg.fs))))
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
		u.queueUpdateDraw(func() { u.startAnalyze(false) })
		return
	}
	u.setStatus("opening the database (waiting if it is locked by another process)")
	if err := u.openSource(); err != nil {
		if errors.Is(err, model.ErrNotAnalyzed) {
			u.setStatus("the filesystem has not been analyzed, running analyze")
			u.queueUpdateDraw(func() { u.startAnalyze(false) })
			return
		}
		u.queueUpdateDraw(func() { u.openFailed(err) })
		return
	}
	u.queueUpdateDraw(func() { u.view.load(u.cfg.target) })
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

// helpPage is the name of the page used to display help, see showHelp.
const helpPage = "help"

// showHelp displays text, which may contain color tags, in a scrollable
// window in the centre of the screen until escape, enter, q or ? is
// pressed. It must be called on the tui's goroutine.
func (u *ui) showHelp(title, text string) {
	focus := u.app.GetFocus()
	tv := tview.NewTextView().SetDynamicColors(true).SetWrap(true).SetWordWrap(true).SetText(text)
	tv.SetBorder(true).SetTitle(" " + title + " help [::d](escape to close)[::-] ")
	tv.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		if ev.Key() == tcell.KeyEscape || ev.Key() == tcell.KeyEnter || ev.Rune() == 'q' || ev.Rune() == '?' {
			u.pages.RemovePage(helpPage)
			u.app.SetFocus(focus)
			return nil
		}
		return ev
	})
	// The window is centred and uses most of the screen.
	grid := tview.NewGrid().SetColumns(0, -8, 0).SetRows(0, -8, 0).AddItem(tv, 1, 1, 1, 1, 0, 0, true)
	u.pages.RemovePage(helpPage)
	u.pages.AddPage(helpPage, grid, true, true)
	u.app.SetFocus(tv)
}

// showError displays err in a dialog, it may be called from any
// goroutine, including the tui's, see displayStatuses.
func (u *ui) showError(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	go u.queueUpdateDraw(func() { u.showDialog(errorDialog, msg) })
}

// dialogOpen returns true if a dialog is displayed.
func (u *ui) dialogOpen() bool {
	return u.pages.HasPage(dialogPage)
}

func (u *ui) openSource() error {
	return u.openSourceWithWarning(true)
}

func (u *ui) openSourceWithWarning(showWarning bool) error {
	src, err := u.cfg.openSource(u.ctx, u.cfg.env, u.cfg.fs, statusWriter{u})
	u.queueUpdateDraw(u.closeWaitDialog)
	if err != nil {
		return err
	}
	sctx, scancel := context.WithCancel(u.ctx)
	h := &sourceHandle{src: src, ctx: sctx, cancel: scancel}
	u.mu.Lock()
	if u.ctx.Err() != nil {
		u.mu.Unlock()
		h.close(u.ctx)
		return u.ctx.Err()
	}
	prev := u.src
	u.src = h
	u.mu.Unlock()
	if prev != nil {
		prev.close(u.ctx)
	}
	u.setStatus("database opened")
	if w, fixable := src.Warning(); showWarning && w != "" {
		u.queueUpdateDraw(func() {
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

func (u *ui) acquireSource(n int) (*sourceHandle, context.Context, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.src == nil {
		return nil, nil, false
	}
	u.src.wg.Add(n)
	return u.src, u.src.ctx, true
}

func (u *ui) source() model.Source {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.src == nil {
		return nil
	}
	return u.src.src
}

func (u *ui) detachSource() *sourceHandle {
	u.mu.Lock()
	h := u.src
	u.src = nil
	u.mu.Unlock()
	if h != nil {
		h.cancel()
	}
	return h
}

func (u *ui) closeSource() {
	u.detachSource().close(u.ctx)
}

// refresh reopens the database and reloads the view.
func (u *ui) refresh() {
	if u.isAnalyzing() {
		return
	}
	u.showView()
	u.view.reset()
	old := u.detachSource()
	go func() {
		old.close(u.ctx)
		if err := u.openSource(); err != nil {
			u.queueUpdateDraw(func() { u.openFailed(err) })
			return
		}
		u.queueUpdateDraw(func() { u.view.load(u.view.currentPath()) })
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
		select {
		case <-done:
		case <-u.ctx.Done():
			return
		}
		u.queueUpdateDraw(u.stop)
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

// showErrors displays the errors recorded for the prefix highlighted in
// the tree and everything beneath it. It must be called on the tui's
// goroutine.
func (u *ui) showErrors() {
	u.errors.show(u.view.currentPath())
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

// queueUpdateDraw runs fn on the tui's goroutine and waits for it,
// returning immediately if the tui has exited.
func (u *ui) queueUpdateDraw(fn func()) {
	if u.ctx.Err() != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		u.app.QueueUpdateDraw(fn)
		close(done)
	}()
	select {
	case <-done:
	case <-u.ctx.Done():
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
			u.queueUpdateDraw(func() { u.status.SetText(tview.Escape(msg)) })
		case <-u.ctx.Done():
			return
		}
	}
}

// waitPage is the name of the page used to display the dialog displayed
// while waiting for a database to open.
const waitPage = "wait"

// openWaitWriter displays the messages written, once a second, while
// waiting for a database to open, eg. because another process has it
// locked, in a dialog, which is closed once the database has opened, ie.
// when an empty line is written. It must not be written to from the tui's
// goroutine.
type openWaitWriter struct{ u *ui }

func (w openWaitWriter) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	w.u.queueUpdateDraw(func() {
		if msg == "" {
			w.u.closeWaitDialog()
			return
		}
		w.u.showWaitDialog(msg)
	})
	return len(p), nil
}

// showWaitDialog displays, or updates, the dialog displayed while waiting
// for a database to open. It must be called on the tui's goroutine.
func (u *ui) showWaitDialog(msg string) {
	text := "The database is locked by another process, eg. an analysis, waiting for it to open:\n\n" + msg
	if u.waitModal != nil {
		u.waitModal.SetText(text)
		return
	}
	u.waitFocus = u.app.GetFocus()
	u.waitModal = tview.NewModal().SetText(text).AddButtons([]string{"Quit"}).
		SetDoneFunc(func(int, string) { u.stop() })
	u.waitModal.SetBorderColor(tcell.ColorYellow).SetTitle(" Waiting ")
	u.pages.AddPage(waitPage, u.waitModal, true, true)
	u.app.SetFocus(u.waitModal)
}

// closeWaitDialog closes the dialog displayed while waiting for a
// database to open, if it is displayed. It must be called on the tui's
// goroutine.
func (u *ui) closeWaitDialog() {
	if u.waitModal == nil {
		return
	}
	u.pages.RemovePage(waitPage)
	u.waitModal = nil
	if u.waitFocus != nil {
		u.app.SetFocus(u.waitFocus)
		u.waitFocus = nil
	}
}

// waiting returns true if the dialog displayed while waiting for a
// database to open is displayed. It must be called on the tui's goroutine.
func (u *ui) waiting() bool {
	return u.waitModal != nil
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
	if u.waiting() || u.dialogOpen() || u.pages.HasPage(helpPage) || u.menu.IsOpen() || u.app.GetFocus() == u.menu {
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
	switch name, _ := u.body.GetFrontPage(); name {
	case viewPage:
		return u.view.input(ev)
	case errorsPage:
		return u.errors.input(ev)
	}
	return u.analyze.input(ev)
}

// runeInput handles the keys that apply to every screen and returns true
// if ev was one of them.
func (u *ui) runeInput(ev *tcell.EventKey) bool {
	switch {
	case ev.Rune() == 'q':
		u.quit()
	case ev.Rune() == 'm' && ev.Modifiers()&tcell.ModAlt != 0:
		u.menu.Activate()
	case ev.Rune() == '/', ev.Rune() == 'm':
		// In the top-N pane / and m select its tab bar, and in the tab bar
		// the main menu.
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
