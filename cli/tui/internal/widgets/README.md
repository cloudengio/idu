# Package [cloudeng.io/cmd/idu/cli/tui/internal/widgets](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/tui/internal/widgets?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/tui/internal/widgets)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/tui/internal/widgets)

```go
import cloudeng.io/cmd/idu/cli/tui/internal/widgets
```

Package widgets provides the tview based widgets used by the tui that tview
does not provide itself, ie. a menu bar with drop-down menus and a set of
tabbed pages.

## Constants
### DropDownPage
```go
const DropDownPage = "menubar-dropdown"
```
DropDownPage is the name of the page used to display drop-down menus.



## Types
### Type MenuBar
```go
type MenuBar struct {
	*tview.TextView
	// contains filtered or unexported fields
}
```
MenuBar is a single row of menu titles, each of which opens a drop-down
menu that is displayed as a page, named DropDownPage, in pages so that it
overlays whatever is displayed beneath the menu bar.

### Functions

```go
func NewMenuBar(app *tview.Application, pages *tview.Pages) *MenuBar
```
NewMenuBar returns a MenuBar whose drop-down menus are displayed in pages.



### Methods

```go
func (mb *MenuBar) Activate()
```
Activate gives the menu bar the focus and selects its first menu.


```go
func (mb *MenuBar) AddAction(title string, action func()) *MenuBar
```
AddAction adds a menu, with the supplied title, that runs action when it is
selected rather than opening a drop-down menu, eg. Quit.


```go
func (mb *MenuBar) AddMenu(title string, items ...MenuItem) *MenuBar
```
AddMenu adds a menu with the supplied title and items.


```go
func (mb *MenuBar) Deactivate()
```
Deactivate closes any drop-down menu and returns the focus to the primitive
that had it before the menu bar was activated.


```go
func (mb *MenuBar) IsOpen() bool
```
IsOpen returns true if a drop-down menu is displayed.


```go
func (mb *MenuBar) Open(i int)
```
Open displays the drop-down menu for menu i.


```go
func (mb *MenuBar) SetRight(text string) *MenuBar
```
SetRight sets the text displayed at the right of the menu bar.


```go
func (mb *MenuBar) Titles() []string
```
Titles returns the titles of the menus.




### Type MenuItem
```go
type MenuItem struct {
	Label  string
	Action func()
}
```
MenuItem is an entry in a drop-down menu.


### Type Tabs
```go
type Tabs struct {
	*tview.Flex
	// contains filtered or unexported fields
}
```
Tabs displays one of a set of primitives, each with a named tab, and a row
of the tabs' names above it.

### Functions

```go
func NewTabs(app *tview.Application) *Tabs
```
NewTabs returns an empty Tabs. The tab bar can be given the focus, see
Activate, in which case left and right select the previous and next tabs and
enter or escape return the focus to the current tab's page. Clicking a tab's
name selects it.



### Methods

```go
func (t *Tabs) Activate()
```
Activate gives the tab bar the focus.


```go
func (t *Tabs) AddTab(name string, p tview.Primitive) *Tabs
```
AddTab adds a tab named name that displays p.


```go
func (t *Tabs) Current() string
```
Current returns the name of the current tab.


```go
func (t *Tabs) IsActive() bool
```
IsActive returns true if the tab bar has the focus.


```go
func (t *Tabs) Names() []string
```
Names returns the names of the tabs.


```go
func (t *Tabs) Next()
```
Next makes the next tab the current one.


```go
func (t *Tabs) Page() tview.Primitive
```
Page returns the primitive displayed for the current tab.


```go
func (t *Tabs) Prev()
```
Prev makes the previous tab the current one.


```go
func (t *Tabs) Select(name string)
```
Select makes the named tab the current one.


```go
func (t *Tabs) SetChangedFunc(fn func(name string)) *Tabs
```
SetChangedFunc sets a function that is called when the current tab changes.







