// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestProgressTracker_DisplayAndCounters(t *testing.T) {
	for _, displayUnchanged := range []bool{false, true} {
		name := "without_unchanged"
		if displayUnchanged {
			name = "with_unchanged"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				wg.Add(1)
				pt := newProgressTracker(ctx, time.Millisecond, true, displayUnchanged, &wg)

				start := pt.Before()
				pt.incStartPrefix()
				pt.incParentUnchanged()
				pt.incChildrenUnchanged()
				pt.incSlowScans()
				pt.incErrors()
				pt.setSyncScans(7)
				pt.setSyncScans(3) // should not overwrite with smaller? (see code_review.md)
				pt.After(start)
				pt.incDonePrefix(2, 10)

				// Advance past the 30s non-interactive display interval.
				time.Sleep(31 * time.Second)
				synctest.Wait()

				cancel()
				wg.Wait()

				s := pt.summarize()
				if s.PrefixesStarted != 1 || s.PrefixesFinished != 1 {
					t.Errorf("prefixes started=%d finished=%d, want 1, 1", s.PrefixesStarted, s.PrefixesFinished)
				}
				if s.Files != 10 || s.PrefixesDeleted != 2 || s.Errors != 1 {
					t.Errorf("unexpected summary counts: %+v", s)
				}
				if s.ParentUnchanged != 1 || s.ChildrenUnchanged != 1 || s.FSStats != 1 {
					t.Errorf("unexpected summary unchanged/stats: %+v", s)
				}
				if s.SlowScans != 1 {
					t.Errorf("SlowScans = %d, want 1", s.SlowScans)
				}
				if s.SynchronousScans != 7 {
					t.Errorf("SynchronousScans = %d, want 7 (must not regress on smaller updates)", s.SynchronousScans)
				}
			})
		})
	}
}
