// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

type phaseProgress struct {
	nullProgress
	mu     sync.Mutex
	phases []string
}

func (p *phaseProgress) Phase(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phases = append(p.phases, name)
}

func (p *phaseProgress) get() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.phases)
}

// TestPhases checks that the phases of an analysis, including those that
// follow the scan of the filesystem and the maintenance performed when the
// database is closed, are reported in order.
func TestPhases(t *testing.T) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	analyze := func(opts ...Option) []string {
		db, err := openTestDB(ctx, dbDir, opts...)
		if err != nil {
			t.Fatal(err)
		}
		pp := &phaseProgress{}
		if err := NewAnalyzer(db, root, WithProgress(pp)).Analyze(ctx, root); err != nil {
			t.Fatal(err)
		}
		if err := db.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); err != nil {
			t.Fatal(err)
		}
		return pp.get()
	}
	// The first analysis with the top-N rankings enabled builds them, and
	// the recursive totals, from all of the summaries.
	topn := WithTopN(2, 3, 1, 2)
	got := analyze(topn)
	want := []string{PhaseDeleteErrors, PhaseScan, PhaseFlush, PhaseRebuildTopN, PhaseValueLogGC}
	if !slices.Equal(got, want) {
		t.Errorf("got %q\nwant %q", got, want)
	}
	// Subsequent ones update them incrementally.
	got = analyze(topn)
	want = []string{PhaseDeleteErrors, PhaseScan, PhaseFlush, PhaseLinks, PhaseSubtrees, PhaseWriteTopN, PhaseValueLogGC}
	if !slices.Equal(got, want) {
		t.Errorf("got %q\nwant %q", got, want)
	}
}
