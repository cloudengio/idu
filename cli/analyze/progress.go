// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"time"

	"cloudeng.io/cmd/idu/idudb"
)

// Summary is the summary of an analysis that is recorded, as JSON, in the
// database log and displayed by the logs command.
type Summary struct {
	Operation            string        `json:"operation"`
	Command              string        `json:"command"`
	Duration             time.Duration `json:"duration"`
	PrefixesStarted      int64         `json:"prefixes_started"`
	PrefixesFinished     int64         `json:"prefixes_finished"`
	SynchronousScans     int64         `json:"synchronous_scans"`
	SlowScans            int64         `json:"slow_scans"`
	FSStats              int64         `json:"fs_stats"`
	FSStatsTotal         int64         `json:"fs_stats_total"`
	FSStatMeanLatency    int64         `json:"fs_stat_mean_latency"`
	Files                int64         `json:"files"`
	ParentUnchanged      int64         `json:"parent_unchanged"`
	ChildrenUnchanged    int64         `json:"children_unchanged"`
	Errors               int64         `json:"errors"`
	PrefixesDeleted      int64         `json:"prefixes_deleted"`
	FilesRestated        int64         `json:"files_restated"`
	FilesRestatedChanged int64         `json:"files_restated_changed"`
}

// Progress is notified of the progress of an analysis and reports on it.
type Progress interface {
	idudb.ScanProgress
	// Stop stops any display of the progress of the analysis and waits
	// for it to finish.
	Stop()
	// Summary returns the progress counters, the caller fills in the
	// Operation, Command and Duration fields.
	Summary() Summary
	// Report displays, and logs, a final summary of the analysis.
	Report(ctx context.Context)
}

// NewProgress creates the Progress for an analysis, display is true if the
// progress should be displayed as the analysis proceeds. Any display must
// stop when ctx is canceled or Stop is called.
type NewProgress func(ctx context.Context, display bool) Progress

// nullProgress is used when no NewProgress is supplied.
type nullProgress struct{}

func (nullProgress) Before() time.Time      { return time.Time{} }
func (nullProgress) After(time.Time)        {}
func (nullProgress) PrefixStarted()         {}
func (nullProgress) PrefixDone(int, int64)  {}
func (nullProgress) Error()                 {}
func (nullProgress) ParentUnchanged()       {}
func (nullProgress) ChildrenUnchanged()     {}
func (nullProgress) SlowScan()              {}
func (nullProgress) SyncScans(int64)        {}
func (nullProgress) Restated(int, int)      {}
func (nullProgress) Stop()                  {}
func (nullProgress) Summary() Summary       { return Summary{} }
func (nullProgress) Report(context.Context) {}
