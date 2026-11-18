// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package topn_test

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"cloudeng.io/cmd/idu/internal/topn"
	"cloudeng.io/cmd/idu/stats"
)

func values(entries []topn.Entry) []int64 {
	v := make([]int64, len(entries))
	for i, e := range entries {
		v[i] = e.Value
	}
	return v
}

func TestHeap(t *testing.T) {
	h := topn.New(3)
	for i, v := range []int64{5, 1, 3, 4} {
		h.Set(fmt.Sprintf("k%d", i), v)
	}
	// k1=1 was evicted when k3=4 was added.
	if got, want := h.Top(0), []topn.Entry{{"k0", 5}, {"k3", 4}, {"k2", 3}}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := h.Threshold(), int64(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := h.Certified(), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Not admitted, but raises the threshold.
	h.Set("k4", 2)
	if _, ok := h.Value("k4"); ok || h.Threshold() != 2 {
		t.Errorf("k4: %v %v", ok, h.Threshold())
	}
	// Shrinking k0 below the threshold makes it uncertified.
	h.Set("k0", 2)
	if got, want := h.Certified(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !h.NeedsRefresh(3) || h.NeedsRefresh(2) {
		t.Errorf("NeedsRefresh: %v %v", h.NeedsRefresh(3), h.NeedsRefresh(2))
	}
	h.Delete("k3")
	h.Set("k2", 0) // zero values are removed
	if got, want := h.Top(0), []topn.Entry{{"k0", 2}}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := h.Top(1); len(got) != 1 {
		t.Errorf("got %v", got)
	}

	// A heap that has never excluded a key never needs a refresh.
	h = topn.New(10)
	h.Set("a", 1)
	if h.NeedsRefresh(5) || h.Certified() != 1 {
		t.Errorf("NeedsRefresh: %v %v", h.NeedsRefresh(5), h.Certified())
	}
}

// TestHeapInvariant checks, against a brute force implementation, that
// every key that is not in the heap has a value no greater than the
// threshold and that the certified entries are the exact top values.
func TestHeapInvariant(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic test data
	for _, capacity := range []int{1, 4, 16} {
		h := topn.New(capacity)
		all := map[string]int64{}
		for i := range 5000 {
			key := fmt.Sprintf("k%d", rnd.IntN(50))
			switch rnd.IntN(4) {
			case 0:
				h.Delete(key)
				delete(all, key)
			default:
				v := rnd.Int64N(1000)
				h.Set(key, v)
				if v > 0 {
					all[key] = v
				} else {
					delete(all, key)
				}
			}
			checkInvariant(t, i, h, all)
		}
	}
}

func checkInvariant(t *testing.T, step int, h *topn.Heap, all map[string]int64) {
	t.Helper()
	for k, v := range all {
		if _, ok := h.Value(k); !ok && v > h.Threshold() {
			t.Fatalf("step %v: %v=%v is not in the heap but exceeds threshold %v", step, k, v, h.Threshold())
		}
	}
	var sorted []int64
	for _, v := range all {
		sorted = append(sorted, v)
	}
	slices.Sort(sorted)
	slices.Reverse(sorted)
	c := h.Certified()
	if got, want := values(h.Top(0)[:c]), sorted[:c]; !slices.Equal(got, want) {
		t.Fatalf("step %v: certified %v: got %v, want %v", step, c, got, want)
	}
}

func TestEncoding(t *testing.T) {
	r := topn.NewRankings(3)
	for i := range 5 {
		r.Set(fmt.Sprintf("/a/%d", i), stats.Totals{Bytes: int64(i + 1), Files: int64(10 - i)})
	}
	r.Delete("/a/4")
	var got topn.Rankings
	if err := got.UnmarshalBinary(r.AppendBinary(nil)); err != nil {
		t.Fatal(err)
	}
	for _, m := range stats.Metrics() {
		if g, w := got[m].Top(0), r[m].Top(0); !slices.Equal(g, w) {
			t.Errorf("%v: got %v, want %v", m, g, w)
		}
		if g, w := got[m].Threshold(), r[m].Threshold(); g != w {
			t.Errorf("%v: got %v, want %v", m, g, w)
		}
		if got[m].Capacity() != 3 {
			t.Errorf("%v: capacity %v", m, got[m].Capacity())
		}
	}
	// The decoded heaps must be usable.
	got[stats.MetricBytes].Set("/b", 100)
	if top := got[stats.MetricBytes].Top(1); top[0].Key != "/b" {
		t.Errorf("got %v", top)
	}
	if err := got.UnmarshalBinary([]byte{0x7f}); err == nil {
		t.Errorf("expected an error")
	}

	s := topn.State{Valid: true, N: 1, Capacity: 2, PerIDN: 3, PerIDCapacity: 4, Users: []int64{1, 20}, Groups: []int64{-1}}
	var gs topn.State
	if err := gs.UnmarshalBinary(s.AppendBinary(nil)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gs, s) {
		t.Errorf("got %+v, want %+v", gs, s)
	}
	if !gs.HasUser(20) || gs.HasUser(2) || !gs.HasGroup(-1) {
		t.Errorf("HasUser/HasGroup")
	}
}
