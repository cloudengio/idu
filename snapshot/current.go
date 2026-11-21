// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

// DefaultN is the number of top entries computed for a snapshot that
// requires a full pass over the summaries, so that it can be used for most
// subsequent views and reports.
const DefaultN = 2000

// NewMatcher returns the matcher for expr, which may be empty, configured
// to deduplicate hardlinks as specified by cfg.
func NewMatcher(ctx context.Context, fwfs filewalk.FS, cfg *iduconfig.Settings, expr ...string) (stats.Matcher, error) {
	return stats.NewMatcher(stats.NewParser(ctx, fwfs),
		stats.WithExpression(expr...),
		stats.WithEmptyValue(true),
		stats.WithFilewalkFS(fwfs),
		stats.WithHardlinkHandling(!cfg.CountHardlinkAsFiles))
}

// Current returns stats, with at least the top n entries, for the entire
// filesystem f. They are, in order of preference:
//
//  1. the most recent snapshot of the entire filesystem in dir, if dir is
//     set, that was created after the database was last updated;
//  2. stats constructed, in memory, from the rankings and recursive totals
//     maintained by analyze, if they are usable;
//  3. stats computed by a full pass over the summaries, which, since they
//     are expensive to compute, are saved as a new snapshot in dir, if
//     set, with at least DefaultN entries.
//
// It returns the path of the snapshot used or saved, which is empty for
// stats constructed in memory or not saved. The database is opened, for
// reading only, using opts, which must include idudb.WithBackendFactory,
// see defaultdb.OpenOptions.
func (c Computer) Current(ctx context.Context, f iduconfig.Filesystem, dir string, n int, opts ...idudb.OpenOption) (File, string, error) {
	cfg := f.Config()
	db, err := idudb.Open(ctx, cfg, opts...)
	if err != nil {
		return File{}, "", fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	defer db.Close(ctx)
	return c.CurrentFrom(ctx, db, f, dir, n)
}

// CurrentFrom is like Current but uses db, the open database for f.
func (c Computer) CurrentFrom(ctx context.Context, db idudb.Reader, f iduconfig.Filesystem, dir string, n int) (File, string, error) {
	cfg := f.Config()
	if dir != "" {
		if updated, ok := lastUpdate(ctx, db); ok {
			sf, path, ok, err := NewStore(dir).FreshFor(cfg.SeparatorOrDefault(), f.Location(), updated, c.Satisfies)
			if err != nil {
				return File{}, "", err
			}
			if ok {
				return sf, path, nil
			}
		}
	}
	if reason, ok := c.needsFullPass(); ok {
		c.logf("computing stats from the summaries: %v\n", reason)
	} else {
		sf, err := c.fromStored(ctx, db, f, n)
		if err == nil {
			return sf, "", nil
		}
		if !errors.Is(err, ErrStoredUnusable) {
			return File{}, "", err
		}
		c.logf("computing stats from the summaries: %v\n", err)
	}
	sf, err := c.fullPass(ctx, db, f, max(n, DefaultN))
	if err != nil || dir == "" {
		return sf, "", err
	}
	path, err := NewStore(dir).Save(sf)
	return sf, path, err
}

// Take computes stats for the entire filesystem f from its database and
// saves them as a new snapshot in dir, returning its path. The stats are
// constructed from the rankings and recursive totals maintained by
// analyze, with topn entries, if they are usable, and by a full pass over
// the summaries otherwise. The database is opened as for Current.
func (c Computer) Take(ctx context.Context, f iduconfig.Filesystem, dir string, opts ...idudb.OpenOption) (string, error) {
	cfg := f.Config()
	db, err := idudb.Open(ctx, cfg, opts...)
	if err != nil {
		return "", fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	defer db.Close(ctx)
	n := cfg.TopNOrDefault()
	sf, err := c.fromStored(ctx, db, f, n)
	if err != nil {
		if !errors.Is(err, ErrStoredUnusable) {
			return "", err
		}
		c.logf("computing the snapshot from the summaries: %v\n", err)
		if sf, err = c.fullPass(ctx, db, f, max(n, DefaultN)); err != nil {
			return "", err
		}
	}
	return NewStore(dir).Save(sf)
}

// lastUpdate returns the time that the database was last updated, ie. by
// the most recent analysis, if known.
func lastUpdate(ctx context.Context, db idudb.Reader) (time.Time, bool) {
	start, stop, _, err := db.LastLog(ctx)
	switch {
	case err != nil:
		return time.Time{}, false
	case !stop.IsZero():
		return stop, true
	case !start.IsZero():
		return start, true
	}
	return time.Time{}, false
}

// fromStored constructs stats for the entire filesystem from the rankings
// and recursive totals maintained by analyze, returning an error that
// wraps ErrStoredUnusable if they cannot be used.
func (c Computer) fromStored(ctx context.Context, db idudb.Reader, f iduconfig.Filesystem, n int) (File, error) {
	cfg := f.Config()
	if !cfg.CountHardlinkAsFiles && !cfg.RecordHardlinks() {
		return File{}, fmt.Errorf("%w: the summaries do not record hardlinks", ErrStoredUnusable)
	}
	match, err := NewMatcher(ctx, localfs.New(), cfg)
	if err != nil {
		return File{}, err
	}
	sdb, perIDN, err := c.FromStored(ctx, db, cfg, f.Location(), n)
	if err != nil {
		return File{}, err
	}
	return File{
		Prefix:     f.Location(),
		Date:       time.Now(),
		Expression: match.String(),
		Stats:      sdb,
		Source:     FromStoredSource,
		Whole:      true,
		PerIDN:     perIDN,
	}, nil
}

// fullPass computes stats for the entire filesystem by a full pass over
// the summaries, or the file metadata.
func (c Computer) fullPass(ctx context.Context, db idudb.Reader, f iduconfig.Filesystem, n int) (File, error) {
	cfg := f.Config()
	match, err := NewMatcher(ctx, localfs.New(), cfg)
	if err != nil {
		return File{}, err
	}
	sdb, source, perIDN, err := c.Compute(ctx, db, cfg, match, f.Location(), false, n)
	if err != nil {
		return File{}, err
	}
	return File{
		Prefix:     f.Location(),
		Date:       time.Now(),
		Expression: match.String(),
		Stats:      sdb,
		Source:     source,
		Whole:      true,
		PerIDN:     perIDN,
	}, nil
}
