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

// subtreeMatch reports whether key equals target or is beneath target
// delimited by sep, and whether lexicographical scan has passed target's
// subtree. Sibling keys sorting between target and target+sep (such as /a-b
// for /a) return (false, false) so scanning skips them and continues to
// /a/... descendants.
func subtreeMatch(key, target, sep string) (match, stop bool) {
	if key == target {
		return true, false
	}
	if strings.HasSuffix(target, sep) {
		if strings.HasPrefix(key, target) {
			return true, false
		}
		return false, true
	}
	childPrefix := target + sep
	if strings.HasPrefix(key, childPrefix) {
		return true, false
	}
	if !strings.HasPrefix(key, target) || key > childPrefix {
		return false, true
	}
	return false, false
}

func (fc *findCmds) findFS(ctx context.Context, fwfs filewalk.FS, ff *findFlags, args []string) error {

	parser := boolexpr.NewParser(ctx, fwfs)

	prefixMatch, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithEmptyEntryValue(true),
		boolexpr.WithFilewalkFS(fwfs),
		boolexpr.WithEntryExpression(ff.Prefix.Values...))
	if err != nil {
		return err
	}

	match, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithEmptyEntryValue(true),
		boolexpr.WithFilewalkFS(fwfs),
		boolexpr.WithEntryExpression(args[1:]...))
	if err != nil {
		return err
	}

	ctx, cfg, target, db, err := openPrefixReadOnly(ctx, globalConfig, args[0])
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	sep := cfg.Separator
	errs := &errors.M{}

	err = db.Scan(ctx, target, func(_ context.Context, k string, v []byte) bool {
		inSubtree, stop := subtreeMatch(k, target, sep)
		if stop {
			return false
		}
		if !inSubtree {
			return true
		}
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			errs.Append(fmt.Errorf("failed to unmarshal value for %v: %v", k, err))
			return false
		}
		if !prefixMatch.Prefix(k, &pi) {
			return true
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
