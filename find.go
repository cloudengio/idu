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
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

type findFlags struct {
	Long   bool            `subcmd:"l,false,'show long listing for each result'"`
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
		xattr := pi.XAttr()
		fmt.Printf("%s uid: %v gid: %v\n", fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, k)), xattr.UID, xattr.GID)
	} else {
		fmt.Printf("%v\n", k)
	}
}

func printEntry(pi prefixinfo.T, fi file.Info, long bool, sep, k string) {
	if long {
		xattr := pi.XAttrInfo(fi)
		fmt.Printf("    %s uid: %v gid: %v\n", fs.FormatFileInfo(fi), xattr.UID, xattr.GID)
	} else {
		n := strings.TrimSuffix(k, sep) + sep + fi.Name()
		fmt.Printf("%v\n", n)
	}
}

func (fc *findCmds) findFS(ctx context.Context, fwfs filewalk.FS, ff *findFlags, args []string) error {

	parser := boolexpr.NewParser(ctx, fwfs)

	match, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithEmptyEntryValue(true),
		boolexpr.WithFilewalkFS(fwfs),
		boolexpr.WithEntryExpression(args[1:]...))
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
			printPrefix(pi, ff.Long, k)
		}
		for _, fi := range pi.InfoList() {
			if fi.IsDir() {
				continue
			}
			if match.Entry(k, &pi, fi) {
				printEntry(pi, fi, ff.Long, sep, k)
			}
		}
		return true
	})
	errs.Append(err)
	return errs.Err()
}
