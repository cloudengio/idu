// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package snapshot provides the statistics, ie. the totals and top-N
// rankings described by report.AllStats, for a filesystem analyzed by idu,
// and the snapshots in which they are stored. It is used by idu's stats,
// reports and tui commands and may be used by other commands to implement
// their own displays or reports of the same statistics.
//
// A snapshot, see File, is written by analyze --snapshot and stats compute
// to a filesystem's snapshot_dir, see Store, in which each snapshot is
// named by the time it was created and a link, named latest, refers to the
// most recent.
//
// Computer.Current returns the current statistics for a filesystem: the
// most recent snapshot that is newer than the last update to its database,
// otherwise statistics constructed from the totals and rankings maintained
// by analyze, otherwise statistics computed by a full pass over the
// summaries, which are saved as a new snapshot. For example:
//
//	cfg, _, err := iduconfig.Load(ctx, &cmdconfig.FilesFlag{})
//	f := cfg.Filesystems[0]
//	sf, path, err := snapshot.Computer{}.Current(ctx, f,
//		f.Config().SnapshotDir, 100, defaultdb.OpenOptions()...)
//	keys, prefixes := report.PopN(sf.Stats.Subtree.Bytes, 10)
//
// Note that the heaps in report.AllStats are consumed as they are read.
package snapshot
