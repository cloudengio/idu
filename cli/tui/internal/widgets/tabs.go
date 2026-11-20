// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package widgets

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// Tabs displays one of a set of primitives, each with a named tab, and a
// row of the tabs' names above it.
type Tabs struct {
	*tview.Flex
	header  *tview.TextView
	pages   *tview.Pages
	names   []string
	current int
	// changed, if set, is called whenever the current tab changes.
	changed func(name string)
	// app is used to move the focus between the tab bar and the pages.
	app *tview.Application
}

// NewTabs returns an empty Tabs. The tab bar can be given the focus, see
// Activate, in which case left and right select the previous and next
// tabs and enter or escape return the focus to the current tab's page.
// Clicking a tab's name selects it.
func NewTabs(app *tview.Application) *Tabs {
	t := &Tabs{
		header: tview.NewTextView().SetDynamicColors(true).SetRegions(true).SetWrap(false),
		pages:  tview.NewPages(),
		app:    app,
	}
	t.header.SetInputCapture(t.input)
	t.header.SetMouseCapture(t.mouse)
	// The focus and blur functions are called with the header's lock held,
	// and hence cannot update it directly.
	rerender := func() { go t.app.QueueUpdateDraw(t.render) }
	t.header.SetFocusFunc(rerender)
	t.header.SetBlurFunc(rerender)
	t.Flex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(t.header, 1, 0, false).
		AddItem(t.pages, 0, 1, true)
	return t
}

// Activate gives the tab bar the focus.
func (t *Tabs) Activate() {
	t.app.SetFocus(t.header)
}

// IsActive returns true if the tab bar has the focus.
func (t *Tabs) IsActive() bool {
	return t.header.HasFocus()
}

// Page returns the primitive displayed for the current tab.
func (t *Tabs) Page() tview.Primitive {
	_, p := t.pages.GetFrontPage()
	return p
}

func (t *Tabs) input(ev *tcell.EventKey) *tcell.EventKey {
	switch ev.Key() {
	case tcell.KeyLeft:
		t.Prev()
	case tcell.KeyRight:
		t.Next()
	case tcell.KeyEnter, tcell.KeyEscape, tcell.KeyDown:
		if p := t.Page(); p != nil {
			t.app.SetFocus(p)
		}
	default:
		return ev
	}
	return nil
}

// offset returns the column at which the name of tab i starts.
func (t *Tabs) offset(i int) int {
	x := 0
	for _, n := range t.names[:i] {
		x += len(n) + 3
	}
	return x
}

func (t *Tabs) mouse(action tview.MouseAction, ev *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
	if action != tview.MouseLeftClick {
		return action, ev
	}
	x, y := ev.Position()
	if !t.header.InRect(x, y) {
		return action, ev
	}
	rx, _, _, _ := t.header.GetInnerRect()
	for i, n := range t.names {
		if start := rx + t.offset(i); x >= start && x < start+len(n)+2 {
			t.show(i)
			if p := t.Page(); p != nil {
				t.app.SetFocus(p)
			}
			return tview.MouseConsumed, nil
		}
	}
	return action, ev
}

// AddTab adds a tab named name that displays p.
func (t *Tabs) AddTab(name string, p tview.Primitive) *Tabs {
	t.names = append(t.names, name)
	t.pages.AddPage(name, p, true, len(t.names) == 1)
	t.render()
	return t
}

// SetChangedFunc sets a function that is called when the current tab
// changes.
func (t *Tabs) SetChangedFunc(fn func(name string)) *Tabs {
	t.changed = fn
	return t
}

// Names returns the names of the tabs.
func (t *Tabs) Names() []string {
	return t.names
}

// Current returns the name of the current tab.
func (t *Tabs) Current() string {
	if len(t.names) == 0 {
		return ""
	}
	return t.names[t.current]
}

// Select makes the named tab the current one.
func (t *Tabs) Select(name string) {
	for i, n := range t.names {
		if n == name {
			t.show(i)
			return
		}
	}
}

// Next makes the next tab the current one.
func (t *Tabs) Next() {
	if len(t.names) > 0 {
		t.show((t.current + 1) % len(t.names))
	}
}

// Prev makes the previous tab the current one.
func (t *Tabs) Prev() {
	if len(t.names) > 0 {
		t.show((t.current + len(t.names) - 1) % len(t.names))
	}
}

func (t *Tabs) show(i int) {
	t.current = i
	t.pages.SwitchToPage(t.names[i])
	t.render()
	if t.changed != nil {
		t.changed(t.names[i])
	}
}

func (t *Tabs) render() {
	var b strings.Builder
	selected := "[black:white]"
	if t.header.HasFocus() {
		selected = "[black:yellow]"
	}
	for i, n := range t.names {
		if i == t.current {
			fmt.Fprintf(&b, "%s %s [-:-] ", selected, n)
		} else {
			fmt.Fprintf(&b, " %s  ", n)
		}
	}
	if t.header.HasFocus() {
		b.WriteString("  [::d]left/right: select  enter: show  m: main menu[::-]")
	}
	t.header.SetText(b.String())
}
