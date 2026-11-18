// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmdutil/flags"
)

type logFlags struct {
	Range flags.TimeInterval `subcmd:"range,,'display the log entries within this time range, written <from>..<to>, <from>.., ..<to> or as a single date/time; eg. -1h.. for the last hour, 2026-11-01..2026-11-05 to include all of the fifth, or 2026-11 for all of November. All entries are displayed if not set'"`
	Erase bool               `subcmd:"erase,false,erase the logs rather than displaying them"`
	JSON  bool               `subcmd:"json,true,display logs in json format"`
}

type errorFlags struct {
	Prefix bool `subcmd:"prefix,false,list errors by prefix"`
	Erase  bool `subcmd:"erase,false,erase the errors rather than displaying them"`
}

type lister struct{}

func errorGroupPrefix(ctx context.Context, db idudb.Reader, key, target, sep string, buf *bytes.Buffer) string {
	if key == target {
		return key
	}
	buf.Reset()
	if err := db.Get(ctx, key, buf); err == nil && buf.Len() > 0 {
		return key
	}
	idx := strings.LastIndex(key, sep)
	if idx < 0 {
		return key
	}
	if idx == 0 {
		return sep
	}
	return key[:idx]
}

func (l *lister) errors(ctx context.Context, values interface{}, args []string) error {
	ef := values.(*errorFlags)
	if ef.Erase {
		return eraseEntries(ctx, prefixArg(args), idudb.EraseErrors)
	}
	ctx, cfg, target, db, err := openPrefixReadOnly(ctx, globalConfig, prefixArg(args))
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	var buf bytes.Buffer
	var lastPrefix string
	return db.VisitErrors(ctx, target,
		func(ctx context.Context, key string, when time.Time, detail []byte) bool {
			inSubtree, stop := subtreeMatch(key, target, cfg.Separator)
			if stop {
				return false
			}
			if !inSubtree {
				return true
			}
			if !ef.Prefix {
				fmt.Printf("%s: %s (%s)\n", key, detail, when.Format(time.RFC3339))
				return true
			}
			prefix := errorGroupPrefix(ctx, db, key, target, cfg.Separator, &buf)
			if prefix != lastPrefix {
				fmt.Printf("%s:\n", prefix)
				lastPrefix = prefix
			}
			fmt.Printf("  %s: %s (%s)\n", key, detail, when.Format(time.RFC3339))
			return true
		})
}

func (l *lister) logs(ctx context.Context, values interface{}, args []string) error {
	lf := values.(*logFlags)
	if lf.Erase {
		return eraseEntries(ctx, prefixArg(args), idudb.EraseLogs)
	}
	ctx, _, _, db, err := openPrefixReadOnly(ctx, globalConfig, prefixArg(args))
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	from, to := logTimeRange(&lf.Range, time.Now())

	return db.VisitLogs(ctx, from, to,
		func(_ context.Context, begin, end time.Time, detail []byte) bool {
			if !lf.JSON {
				fmt.Printf("%v...%v: %v: %s\n", begin, end, end.Sub(begin), detail)
				return true
			}
			var summary struct {
				Begin time.Time `json:"begin"`
				End   time.Time `json:"end"`
				anaylzeSummary
			}
			if err := json.Unmarshal(detail, &summary); err != nil {
				fmt.Fprintf(os.Stderr, "failed to unmarshal log %v entry: %v\n", begin, err)
				return true
			}
			summary.Begin = begin
			summary.End = end
			out, _ := json.Marshal(summary)
			fmt.Println(string(out))
			return true

		})
}

// logTimeRange returns the inclusive range of times, [from, to], to be passed
// to idudb.Reader.VisitLogs for the half-open interval [From, To) specified
// by iv. An open start is the zero time and an open end, or an unset
// interval, is now.
func logTimeRange(iv *flags.TimeInterval, now time.Time) (from, to time.Time) {
	from, _ = iv.From()
	if t, ok := iv.To(); ok {
		return from, t.Add(-time.Nanosecond)
	}
	return from, now
}
