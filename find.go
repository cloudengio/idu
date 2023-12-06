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
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/errors"
)

type findFlags struct {
	Long bool `subcmd:"long,false,'show long listing for each result'"`
}

type findCmds struct{}

type findHandler struct {
	sep  string
	expr iduExpr
	long bool
}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	ff := values.(*findFlags)

	expr, err := createExpr(args[1:])
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

		named := prefixinfo.NewNamed(k, pi)
		if expr.Eval(named) {
			if ff.Long {
				fmt.Println(fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, k)))
			} else {
				fmt.Printf("%v/\n", k)
			}
		}
		for _, fi := range pi.InfoList() {
			uid, gid := pi.UserGroupInfo(fi)
			fid := fileWithID{Info: fi, uid: uid, gid: gid}
			n := fid.Name()
			if expr.Eval(fid) {
				if ff.Long {
					fmt.Println("    ", fs.FormatFileInfo(fi))
				} else {
					fmt.Printf("%v\n", k+sep+n)
				}
			}
		}
		return true
	})
	errs.Append(err)
	return errs.Err()
}
