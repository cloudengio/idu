// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/cli/tui/internal/model"
	"cloudeng.io/cmd/idu/internal/cli"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// errorsPage is the name of the page that displays the errors recorded
// in the database.
const errorsPage = "errors"

// errorsLimit is the maximum number of errors displayed.
const errorsLimit = 10000

// errorsScreen displays the errors recorded by the most recent analyses
// for a prefix and everything beneath it.
type errorsScreen struct {
	u      *ui
	layout *tview.Flex
	table  *tview.Table
	detail *tview.TextView

	mu sync.Mutex
	// generation is incremented whenever the errors are loaded so that
	// the results of stale loads are discarded.
	generation int
}

func newErrorsScreen(u *ui) *errorsScreen {
	e := &errorsScreen{u: u}
	e.table = tview.NewTable().SetSelectable(true, false).SetFixed(1, 0)
	e.table.SetBorder(true).SetTitle(" Errors ")
	e.table.SetSelectedFunc(func(row, _ int) { e.gotoRow(row) })
	e.table.SetSelectionChangedFunc(func(row, _ int) { e.showDetail(row) })
	e.detail = tview.NewTextView().SetDynamicColors(true).SetWrap(true)
	e.detail.SetBorder(true).SetTitle(" Error ")
	e.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(e.table, 0, 1, true).
		AddItem(e.detail, 6, 0, false)
	return e
}

// show displays the errors for path and everything beneath it. It must be
// called on the tui's goroutine.
func (e *errorsScreen) show(path string) {
	e.u.body.SwitchToPage(errorsPage)
	e.u.app.SetFocus(e.table)
	e.mu.Lock()
	e.generation++
	gen := e.generation
	e.mu.Unlock()
	e.table.Clear()
	e.detail.SetText("")
	title := fmt.Sprintf(" Errors beneath %v", tview.Escape(path))
	h, ctx, ok := e.u.acquireSource(1)
	if !ok {
		e.table.SetTitle(title + " ")
		e.detail.SetText("The database is not open.")
		return
	}
	e.table.SetTitle(title + ": loading... ")
	go func() {
		errs, more, err := h.src.Errors(ctx, path, errorsLimit)
		h.release()
		if ctx.Err() != nil {
			return
		}
		e.u.queueUpdateDraw(func() {
			e.mu.Lock()
			current := gen == e.generation
			e.mu.Unlock()
			if !current {
				return
			}
			if err != nil {
				e.table.SetTitle(title + " ")
				e.detail.SetText("[red]" + tview.Escape(err.Error()))
				return
			}
			e.fill(path, title, errs, more)
		})
	}()
}

func (e *errorsScreen) fill(path, title string, errs []model.ErrorEntry, more bool) {
	n := strings.TrimSpace(cli.FmtCount(int64(len(errs))))
	switch {
	case more:
		title += fmt.Sprintf(": the first %v ", n)
	default:
		title += fmt.Sprintf(": %v ", n)
	}
	e.table.SetTitle(title)
	if len(errs) == 0 {
		e.detail.SetText(fmt.Sprintf("No errors were recorded for %v by the most recent analyses.", tview.Escape(path)))
		return
	}
	header(e.table, "When", "Path", "Error")
	for i, er := range errs {
		e.table.SetCell(i+1, 0, tview.NewTableCell(er.When.Local().Format(time.DateTime)).SetReference(er))
		e.table.SetCell(i+1, 1, tview.NewTableCell(tview.Escape(relativeTo(path, er.Path))))
		e.table.SetCell(i+1, 2, tview.NewTableCell(tview.Escape(firstLine(er.Message))).SetExpansion(1))
	}
	e.table.Select(1, 0).ScrollToBeginning()
	e.showDetail(1)
}

// relativeTo returns path relative to prefix, if it is beneath it.
func relativeTo(prefix, path string) string {
	if isAncestor(prefix, path) {
		return strings.TrimLeft(path[len(prefix):], `/\`)
	}
	return path
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	return s
}

// entry returns the error displayed in row, if any.
func (e *errorsScreen) entry(row int) (model.ErrorEntry, bool) {
	er, ok := e.table.GetCell(row, 0).GetReference().(model.ErrorEntry)
	return er, ok
}

func (e *errorsScreen) showDetail(row int) {
	er, ok := e.entry(row)
	if !ok {
		return
	}
	e.detail.SetText(fmt.Sprintf("[::b]%v[::-] (%v)\n%v",
		tview.Escape(er.Path), er.When.Local().Format(time.RFC3339), tview.Escape(strings.TrimSpace(er.Message)))).ScrollToBeginning()
}

// gotoRow highlights the prefix of the error in row, or its closest
// ancestor if it is a file or was not analyzed, in the tree.
func (e *errorsScreen) gotoRow(row int) {
	er, ok := e.entry(row)
	if !ok {
		return
	}
	e.u.showView()
	e.u.view.expandTo(e.u.view.tree.GetRoot(), er.Path)
}

func (e *errorsScreen) input(ev *tcell.EventKey) *tcell.EventKey {
	switch ev.Key() {
	case tcell.KeyEscape:
		e.u.showView()
		return nil
	case tcell.KeyTab, tcell.KeyBacktab, tcell.KeyLeft, tcell.KeyRight:
		if e.u.app.GetFocus() == e.table {
			e.u.app.SetFocus(e.detail)
		} else {
			e.u.app.SetFocus(e.table)
		}
		return nil
	}
	return ev
}
