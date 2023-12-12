// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/errors"
)

type findFlags struct {
	Long            bool `subcmd:"long,false,'show long listing for each result'"`
	HandleHardlinks bool `subcmd:"handle-hardlinks,false,'handle hardlinks'"`
}

type findCmds struct{}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	ff := values.(*findFlags)

	parser := boolexpr.NewParser()

	match, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithExpression(args[1:]...),
		boolexpr.WithHardlinkHandling(ff.HandleHardlinks))
	if err != nil {
		return err
	}

	ctx, cfg, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	sep := cfg.Separator
	errs := &errors.M{}

	err = db.Scan(ctx, args[0], func(_ context.Context, k string, v []byte) bool {
		if !strings.HasPrefix(k, args[0]) {
			return false
		}

		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			errs.Append(fmt.Errorf("failed to unmarshal value for %v: %v", k, err))
			return false
		}
		if match.Prefix(k, &pi) {
			if ff.Long {
				fmt.Println(fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, k)))
			} else {
				fmt.Printf("%v/\n", k)
			}
		}
		for _, fi := range pi.InfoList() {
			if match.Entry(k, &pi, fi) {
				if ff.Long {
					fmt.Println("    ", fs.FormatFileInfo(fi))
				} else {
					fmt.Printf("%v\n", k+sep+fi.Name())
				}
			}
		}
		return true
	})
	errs.Append(err)
	return errs.Err()
}
