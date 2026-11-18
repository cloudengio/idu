// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package topn provides bounded, incrementally maintainable, top-N heaps
// and their encoding for storage in an idu database.
//
// A Heap holds at most capacity entries, typically an oversubscribed
// multiple of the N entries of interest, so that entries which shrink or
// are removed can be replaced without recomputing the heap from scratch.
// It also records a threshold, τ, that is an upper bound on the value of
// every key that is not in the heap: whenever a key is evicted, or is not
// admitted because the heap is full, τ is raised to cover its value.
// Since a key whose value grows beyond the heap's minimum is always
// admitted, every entry with a value greater than τ is guaranteed to be in
// its exact position relative to all keys, including those not in the
// heap. The number of such entries is reported by Certified.
package topn

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
)

// Entry is a single key and its value.
type Entry struct {
	Key   string
	Value int64
}

// Heap is a bounded min-heap of entries indexed by key. It is not safe for
// concurrent use.
type Heap struct {
	capacity  int
	threshold int64
	h         entries
}

type entries struct {
	e     []Entry
	index map[string]int
}

func (h *entries) Len() int           { return len(h.e) }
func (h *entries) Less(i, j int) bool { return h.e[i].Value < h.e[j].Value }
func (h *entries) Swap(i, j int) {
	h.e[i], h.e[j] = h.e[j], h.e[i]
	h.index[h.e[i].Key] = i
	h.index[h.e[j].Key] = j
}

func (h *entries) Push(x any) {
	e := x.(Entry)
	h.index[e.Key] = len(h.e)
	h.e = append(h.e, e)
}

func (h *entries) Pop() any {
	n := len(h.e) - 1
	e := h.e[n]
	h.e = h.e[:n]
	delete(h.index, e.Key)
	return e
}

// New returns a Heap that holds at most capacity entries.
func New(capacity int) *Heap {
	return &Heap{
		capacity: max(capacity, 0),
		h:        entries{index: map[string]int{}},
	}
}

// Capacity returns the maximum number of entries held by the heap.
func (h *Heap) Capacity() int {
	return h.capacity
}

// Len returns the number of entries in the heap.
func (h *Heap) Len() int {
	return len(h.h.e)
}

// Threshold returns τ, an upper bound on the value of every key that is
// not in the heap.
func (h *Heap) Threshold() int64 {
	return h.threshold
}

func (h *Heap) raise(v int64) {
	h.threshold = max(h.threshold, v)
}

// Set records that the value of key is now v. Keys with values that are
// zero or less are removed since they are never of interest.
func (h *Heap) Set(key string, v int64) {
	if v <= 0 {
		h.Delete(key)
		return
	}
	if i, ok := h.h.index[key]; ok {
		h.h.e[i].Value = v
		heap.Fix(&h.h, i)
		return
	}
	if len(h.h.e) < h.capacity {
		heap.Push(&h.h, Entry{Key: key, Value: v})
		return
	}
	if h.capacity == 0 || v <= h.h.e[0].Value {
		h.raise(v)
		return
	}
	evicted := h.h.e[0]
	h.raise(evicted.Value)
	delete(h.h.index, evicted.Key)
	h.h.e[0] = Entry{Key: key, Value: v}
	h.h.index[key] = 0
	heap.Fix(&h.h, 0)
}

// Delete removes key from the heap, if present. This does not change the
// threshold since key no longer has a value.
func (h *Heap) Delete(key string) {
	if i, ok := h.h.index[key]; ok {
		heap.Remove(&h.h, i)
	}
}

// Value returns the value of key if it is in the heap.
func (h *Heap) Value(key string) (int64, bool) {
	i, ok := h.h.index[key]
	if !ok {
		return 0, false
	}
	return h.h.e[i].Value, true
}

// Certified returns the number of entries whose value is greater than the
// threshold and which are hence known to be ranked exactly.
func (h *Heap) Certified() int {
	n := 0
	for _, e := range h.h.e {
		if e.Value > h.threshold {
			n++
		}
	}
	return n
}

// NeedsRefresh returns true if fewer than n entries are known to be
// ranked exactly, ie. if the heap can no longer be trusted for a top-n
// and must be recomputed. A heap whose threshold is zero has never had
// a key excluded and is therefore always exact.
func (h *Heap) NeedsRefresh(n int) bool {
	return h.threshold > 0 && h.Certified() < n
}

// Top returns up to n entries, or all entries if n is zero or less, in
// descending order of value, with ties broken by key.
func (h *Heap) Top(n int) []Entry {
	out := slices.Clone(h.h.e)
	slices.SortFunc(out, func(a, b Entry) int {
		if a.Value != b.Value {
			if a.Value > b.Value {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Key, b.Key)
	})
	if n > 0 && n < len(out) {
		out = out[:n]
	}
	return out
}

// AppendBinary appends the binary encoding of h to data.
func (h *Heap) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, uint64(h.capacity))
	data = binary.AppendVarint(data, h.threshold)
	data = binary.AppendUvarint(data, uint64(len(h.h.e)))
	for _, e := range h.Top(0) {
		data = binary.AppendUvarint(data, uint64(len(e.Key)))
		data = append(data, e.Key...)
		data = binary.AppendVarint(data, e.Value)
	}
	return data
}

// DecodeBinary decodes a Heap encoded by AppendBinary from data and returns
// the remaining data.
func (h *Heap) DecodeBinary(data []byte) ([]byte, error) {
	capacity, data, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("capacity: %w", err)
	}
	threshold, data, err := readVarint(data)
	if err != nil {
		return nil, fmt.Errorf("threshold: %w", err)
	}
	n, data, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("length: %w", err)
	}
	if n > capacity || n > uint64(len(data)) {
		return nil, fmt.Errorf("invalid length %d for capacity %d", n, capacity)
	}
	*h = *New(int(capacity))
	h.threshold = threshold
	h.h.e = make([]Entry, 0, n)
	for range n {
		var kl uint64
		if kl, data, err = readUvarint(data); err != nil {
			return nil, fmt.Errorf("key length: %w", err)
		}
		if kl > uint64(len(data)) {
			return nil, fmt.Errorf("truncated key")
		}
		key := string(data[:kl])
		data = data[kl:]
		var v int64
		if v, data, err = readVarint(data); err != nil {
			return nil, fmt.Errorf("value: %w", err)
		}
		if _, dup := h.h.index[key]; dup {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		h.h.Push(Entry{Key: key, Value: v})
	}
	heap.Init(&h.h)
	return data, nil
}

func readUvarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("truncated uvarint")
	}
	return v, data[n:], nil
}

func readVarint(data []byte) (int64, []byte, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("truncated varint")
	}
	return v, data[n:], nil
}
