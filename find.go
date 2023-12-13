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
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/localfs"
)

type findFlags struct {
	Long            bool `subcmd:"long,false,'show long listing for each result'"`
	HandleHardlinks bool `subcmd:"handle-hardlinks,false,'handle hardlinks'"`
}

type findCmds struct{}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return fc.findFS(ctx, fs, values.(*findFlags), args)
}

func (fc *findCmds) findFS(ctx context.Context, fwfs filewalk.FS, ff *findFlags, args []string) error {

	parser := boolexpr.NewParser(fwfs)

	match, err := boolexpr.CreateMatcher(parser, boolexpr.WithExpression(args[1:]...))
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
		if k == args[0] {
			n := strings.TrimSuffix(k, sep)
			if ff.Long {
				fmt.Println(fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, n)))
			} else {
				fmt.Printf("%v\n", n)
			}
		}
		for _, fi := range pi.InfoList() {
			if match.Entry(k, &pi, fi) {
				if ff.Long {
					fmt.Println("    ", fs.FormatFileInfo(fi))
				} else {
					n := strings.TrimSuffix(k, sep) + sep + fi.Name()
					fmt.Printf("%v\n", n)
				}
			}
		}
		return true
	})
	errs.Append(err)
	return errs.Err()
}
