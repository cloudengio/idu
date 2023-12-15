// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"strings"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/localfs"
)

type findFlags struct {
	Long   bool            `subcmd:"long,false,'show long listing for each result'"`
	Prefix flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}

type findCmds struct{}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return fc.findFS(ctx, fs, values.(*findFlags), args)
}

func printPrefix(pi prefixinfo.T, long bool, k string) {
	if long {
		fmt.Println(fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, k)))
	} else {
		fmt.Printf("%v\n", k)
	}
}

func printEntry(pi prefixinfo.T, fi file.Info, long bool, sep, k string) {
	if long {
		fmt.Println("    ", fs.FormatFileInfo(fi))
	} else {
		n := strings.TrimSuffix(k, sep) + sep + fi.Name()
		fmt.Printf("%v\n", n)
	}
}

func handleDirEntry(ctx context.Context, db database.DB, match boolexpr.Matcher, pi *prefixinfo.T, fi file.Info, long bool, k, nk string) (bool, error) {
	var buf bytes.Buffer
	if err := db.Get(ctx, nk, &buf); err != nil {
		return false, fmt.Errorf("failed to fetch directory entry for %v: %v", k, err)
	}
	npi := prefixinfo.T{}
	if err := npi.UnmarshalBinary(buf.Bytes()); err != nil {
		return false, fmt.Errorf("failed to unmarshal directory entry for %v: %v", k, err)
	}
	return match.Prefix(nk, &npi) && match.Entry(k, pi, fi), nil
}

func (fc *findCmds) findFS(ctx context.Context, fwfs filewalk.FS, ff *findFlags, args []string) error {

	parser := boolexpr.NewParser(fwfs)

	match, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithEntryExpression(args[1:]...),
		boolexpr.WithPrefixExpression(ff.Prefix.Values...))
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
		if !match.IsPrefixSet() && args[0] == k {
			printPrefix(pi, ff.Long, k)
		}
		for _, fi := range pi.InfoList() {
			if match.IsPrefixSet() && fi.IsDir() && k != args[0] {
				nextPrefix := k + sep + fi.Name()
				// Need to fetch the directory entry to see if it matches or not
				matched, err := handleDirEntry(ctx, db, match, &pi, fi, ff.Long, k, nextPrefix)
				if err != nil {
					errs.Append(err)
					return false
				}
				if matched {
					printPrefix(pi, ff.Long, nextPrefix)
				}
				continue
			}
			if !match.IsPrefixSet() && match.Entry(k, &pi, fi) {
				printEntry(pi, fi, ff.Long, sep, k)
			}

		}
		return true
	})
	errs.Append(err)
	return errs.Err()
}
