// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package widgets provides the tview based widgets used by the tui that
// tview does not provide itself, ie. a menu bar with drop-down menus and a
// set of tabbed pages.
package widgets

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// MenuItem is an entry in a drop-down menu.
type MenuItem struct {
	Label  string
	Action func()
}

type menu struct {
	title string
	items []MenuItem
	// action, if set, is run when the menu is selected rather than
	// opening a drop-down.
	action func()
}

// MenuBar is a single row of menu titles, each of which opens a drop-down
// menu that is displayed as a page, named DropDownPage, in pages so that
// it overlays whatever is displayed beneath the menu bar.
type MenuBar struct {
	*tview.TextView
	pages    *tview.Pages
	app      *tview.Application
	menus    []menu
	selected int
	// restore is the primitive that had the focus before the menu bar.
	restore tview.Primitive
	// right is displayed right-aligned, eg. key hints.
	right string
}

// DropDownPage is the name of the page used to display drop-down menus.
const DropDownPage = "menubar-dropdown"

// NewMenuBar returns a MenuBar whose drop-down menus are displayed in
// pages.
func NewMenuBar(app *tview.Application, pages *tview.Pages) *MenuBar {
	mb := &MenuBar{
		TextView: tview.NewTextView().SetDynamicColors(true).SetRegions(true).SetWrap(false),
		pages:    pages,
		app:      app,
		selected: -1,
	}
	mb.SetInputCapture(mb.input)
	mb.SetMouseCapture(mb.mouse)
	return mb
}

// mouse opens the menu whose title is clicked.
func (mb *MenuBar) mouse(action tview.MouseAction, ev *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
	if action != tview.MouseLeftClick {
		return action, ev
	}
	x, y := ev.Position()
	if !mb.InRect(x, y) {
		return action, ev
	}
	rx, _, _, _ := mb.GetInnerRect()
	for i := range mb.menus {
		start := rx + mb.offset(i)
		if x >= start && x < start+len(mb.menus[i].title)+2 {
			mb.Open(i)
			return tview.MouseConsumed, nil
		}
	}
	return action, ev
}

// AddMenu adds a menu with the supplied title and items.
func (mb *MenuBar) AddMenu(title string, items ...MenuItem) *MenuBar {
	mb.menus = append(mb.menus, menu{title: title, items: items})
	mb.render()
	return mb
}

// AddAction adds a menu, with the supplied title, that runs action when
// it is selected rather than opening a drop-down menu, eg. Quit.
func (mb *MenuBar) AddAction(title string, action func()) *MenuBar {
	mb.menus = append(mb.menus, menu{title: title, action: action})
	mb.render()
	return mb
}

// SetRight sets the text displayed at the right of the menu bar.
func (mb *MenuBar) SetRight(text string) *MenuBar {
	mb.right = text
	mb.render()
	return mb
}

// Titles returns the titles of the menus.
func (mb *MenuBar) Titles() []string {
	t := make([]string, len(mb.menus))
	for i, m := range mb.menus {
		t[i] = m.title
	}
	return t
}

func (mb *MenuBar) render() {
	var b strings.Builder
	for i, m := range mb.menus {
		if i == mb.selected {
			fmt.Fprintf(&b, `["%d"][black:white] %s [-:-][""]`, i, m.title)
		} else {
			fmt.Fprintf(&b, `["%d"] %s [""]`, i, m.title)
		}
		b.WriteString(" ")
	}
	if mb.right != "" {
		b.WriteString("   ")
		b.WriteString(mb.right)
	}
	mb.SetText(b.String())
}

// offset returns the column at which the title of menu i starts.
func (mb *MenuBar) offset(i int) int {
	x := 0
	for _, m := range mb.menus[:i] {
		x += len(m.title) + 3
	}
	return x
}

// Activate gives the menu bar the focus and selects its first menu.
func (mb *MenuBar) Activate() {
	if len(mb.menus) == 0 {
		return
	}
	if mb.selected < 0 {
		mb.restore = mb.app.GetFocus()
		mb.selected = 0
	}
	mb.render()
	mb.app.SetFocus(mb)
}

// Deactivate closes any drop-down menu and returns the focus to the
// primitive that had it before the menu bar was activated.
func (mb *MenuBar) Deactivate() {
	mb.pages.RemovePage(DropDownPage)
	mb.selected = -1
	mb.render()
	if mb.restore != nil {
		mb.app.SetFocus(mb.restore)
		mb.restore = nil
	}
}

// Open displays the drop-down menu for menu i.
func (mb *MenuBar) Open(i int) {
	if i < 0 || i >= len(mb.menus) {
		return
	}
	if mb.selected < 0 {
		mb.restore = mb.app.GetFocus()
	}
	mb.selected = i
	mb.render()
	m := mb.menus[i]
	if m.action != nil {
		mb.Deactivate()
		m.action()
		return
	}
	list := tview.NewList().ShowSecondaryText(false)
	list.SetBorder(true)
	width := len(m.title) + 4
	for _, item := range m.items {
		action := item.Action
		list.AddItem(item.Label, "", 0, func() {
			mb.Deactivate()
			if action != nil {
				action()
			}
		})
		width = max(width, len(item.Label)+4)
	}
	list.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		switch ev.Key() {
		case tcell.KeyEscape:
			mb.Deactivate()
			return nil
		case tcell.KeyLeft:
			mb.Open((mb.selected + len(mb.menus) - 1) % len(mb.menus))
			return nil
		case tcell.KeyRight:
			mb.Open((mb.selected + 1) % len(mb.menus))
			return nil
		}
		return ev
	})
	// Nil items leave whatever is beneath them visible.
	overlay := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(nil, 1, 0, false).
		AddItem(tview.NewFlex().
			AddItem(nil, mb.offset(i), 0, false).
			AddItem(list, width, 0, true).
			AddItem(nil, 0, 1, false), len(m.items)+2, 0, true).
		AddItem(nil, 0, 1, false)
	mb.pages.RemovePage(DropDownPage)
	mb.pages.AddPage(DropDownPage, overlay, true, true)
	mb.app.SetFocus(list)
}

// IsOpen returns true if a drop-down menu is displayed.
func (mb *MenuBar) IsOpen() bool {
	return mb.pages.HasPage(DropDownPage)
}

func (mb *MenuBar) input(ev *tcell.EventKey) *tcell.EventKey {
	switch ev.Key() {
	case tcell.KeyLeft:
		mb.selected = (mb.selected + len(mb.menus) - 1) % len(mb.menus)
		mb.render()
		return nil
	case tcell.KeyRight:
		mb.selected = (mb.selected + 1) % len(mb.menus)
		mb.render()
		return nil
	case tcell.KeyEnter, tcell.KeyDown:
		mb.Open(mb.selected)
		return nil
	case tcell.KeyEscape:
		mb.Deactivate()
		return nil
	}
	return ev
}
