// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"cloudeng.io/cmd/idu/stats"
)

// fakeSource serves canned listings and counts calls to List.
type fakeSource struct {
	listings map[string]Listing
	calls    map[string]int
}

func (f *fakeSource) List(_ context.Context, path string) (Listing, error) {
	f.calls[path]++
	l, ok := f.listings[path]
	if !ok {
		return Listing{}, fmt.Errorf("no such directory: %v", path)
	}
	l.Children = append([]Entry(nil), l.Children...) // the model sorts in place.
	return l, nil
}

func dir(path, name string, storage, files, dirs int64) Entry {
	return Entry{Name: name, Path: path, Dirs: dirs,
		Tree: stats.Totals{StorageBytes: storage, Bytes: storage, Files: files}}
}

func newFake() *fakeSource {
	return &fakeSource{
		calls: map[string]int{},
		listings: map[string]Listing{
			"/r": {
				Self: dir("/r", "/r", 600, 60, 4),
				Children: []Entry{
					dir("/r/a", "a", 100, 50, 0),
					dir("/r/b", "b", 500, 10, 2),
				},
			},
			"/r/b": {
				Self: dir("/r/b", "/r/b", 500, 10, 2),
				Children: []Entry{
					dir("/r/b/x", "x", 200, 4, 0),
					dir("/r/b/y", "y", 300, 6, 0),
				},
			},
		},
	}
}

// run executes cmd, if any, and delivers the resulting message to m as
// the Bubble Tea runtime would.
func run(t *testing.T, m *Model, cmd tea.Cmd) {
	t.Helper()
	if cmd == nil {
		return
	}
	m.Update(cmd())
}

func press(t *testing.T, m *Model, key string) {
	t.Helper()
	var k tea.KeyPressMsg
	switch key {
	case "enter":
		k = tea.KeyPressMsg{Code: tea.KeyEnter}
	case "backspace":
		k = tea.KeyPressMsg{Code: tea.KeyBackspace}
	case "down":
		k = tea.KeyPressMsg{Code: tea.KeyDown}
	default:
		k = tea.KeyPressMsg{Code: rune(key[0]), Text: key}
	}
	_, cmd := m.Update(k)
	run(t, m, cmd)
}

func names(m *Model) string {
	var n []string
	for _, e := range m.children() {
		n = append(n, e.Name)
	}
	return strings.Join(n, ",")
}

func selected(t *testing.T, m *Model) string {
	t.Helper()
	e, ok := m.Selected()
	if !ok {
		t.Fatalf("%v: nothing selected", m.Path())
	}
	return e.Name
}

func TestNavigation(t *testing.T) {
	src := newFake()
	m := New(context.Background(), src, "/r", Options{})
	run(t, m, m.Init())

	if got, want := names(m), "b,a"; got != want {
		t.Errorf("got %v, want %v (sorted by storage)", got, want)
	}
	if got, want := selected(t, m), "b"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	press(t, m, "enter")
	if got, want := m.Path(), "/r/b"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := names(m), "y,x"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// x has no sub-directories so cannot be opened.
	press(t, m, "down")
	press(t, m, "enter")
	if got, want := m.Path(), "/r/b"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Returning to the root restores the selection, without reloading.
	press(t, m, "backspace")
	if got, want := m.Path(), "/r"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := selected(t, m), "b"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	press(t, m, "backspace") // no-op at the root.
	if got, want := m.Path(), "/r"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	press(t, m, "enter")
	if got, want := src.calls["/r"]+src.calls["/r/b"], 2; got != want {
		t.Errorf("got %v calls to List, want %v", got, want)
	}

	// Refresh reloads the current directory.
	press(t, m, "r")
	if got, want := src.calls["/r/b"], 2; got != want {
		t.Errorf("got %v calls to List, want %v", got, want)
	}
}

func TestSort(t *testing.T) {
	m := New(context.Background(), newFake(), "/r", Options{})
	run(t, m, m.Init())
	for _, want := range []string{"b,a", "a,b", "a,b", "b,a"} { // bytes, files, name, storage
		press(t, m, "s")
		if got := names(m); got != want {
			t.Errorf("sort by %v: got %v, want %v", m.sortBy, got, want)
		}
	}
}

func TestRowColumns(t *testing.T) {
	m := New(context.Background(), newFake(), "/r", Options{})
	run(t, m, m.Init())
	a := m.children()[1]
	parent := m.cache["/r"].Self
	// Sorted by storage: storage, then the number of files.
	if got, want := strings.Fields(m.row(a, parent)), []string{"100", "B", "██········", "50", "a"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	// Sorted by files: the number of files, then storage.
	m.sortBy = ByFiles
	if got, want := strings.Fields(m.row(a, parent)), []string{"50", "████████··", "100", "B", "a"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestStaleListing(t *testing.T) {
	src := newFake()
	m := New(context.Background(), src, "/r", Options{})
	run(t, m, m.Init())

	// Open b, but leave before its listing arrives.
	_, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	press(t, m, "backspace")
	run(t, m, cmd)
	if got, want := m.Path(), "/r"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// The listing is cached for when b is next opened.
	press(t, m, "enter")
	if got, want := src.calls["/r/b"], 1; got != want {
		t.Errorf("got %v calls to List, want %v", got, want)
	}
}

func TestErrorAndView(t *testing.T) {
	src := newFake()
	delete(src.listings, "/r/b")
	m := New(context.Background(), src, "/r", Options{})
	m.Update(tea.WindowSizeMsg{Width: 60, Height: 10})
	run(t, m, m.Init())

	out := m.View().Content
	for _, want := range []string{"idu  /r", "600 B in 60 files, 4 dirs", "b/", "a"} {
		if !strings.Contains(out, want) {
			t.Errorf("view %q does not contain %q", out, want)
		}
	}
	press(t, m, "enter")
	if out := m.View().Content; !strings.Contains(out, "no such directory: /r/b") {
		t.Errorf("view %q does not contain the error", out)
	}
}
