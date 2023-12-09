// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloudeng.io/cmd/idu/internal"
)

type logFlags struct {
	internal.TimeRangeFlags
	Erase bool `subcmd:"erase,false,erase the logs rather than displaying them"`
	JSON  bool `subcmd:"json,true,display logs in json format"`
}

type errorFlags struct {
	Prefix bool `subcmd:"prefix,false,list errors by prefix"`
	Erase  bool `subcmd:"erase,false,erase the errors rather than displaying them"`
}

type lister struct{}

func (l *lister) errors(ctx context.Context, values interface{}, args []string) error {
	ef := values.(*errorFlags)
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	if ef.Erase {
		return db.Clear(ctx, false, true)
	}

	return db.VisitErrors(ctx, args[0],
		func(_ context.Context, key string, when time.Time, detail []byte) bool {
			fmt.Printf("%s: %s\n", key, detail)
			return true
		})
}

func (l *lister) logs(ctx context.Context, values interface{}, args []string) error {
	lf := values.(*logFlags)
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	if lf.Erase {
		return db.Clear(ctx, false, true)
	}

	from, to, _, err := lf.TimeRangeFlags.FromTo()
	if err != nil {
		return err
	}

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
