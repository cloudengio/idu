// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmdutil/flags"
)

type findFlags struct {
	User        string          `subcmd:"user,,restrict output to the specified user"`
	Group       string          `subcmd:"group,,restrict output to the specified group"`
	PrefixMatch flags.Repeating `subcmd:"prefix,,'a regular expression to match against prefixes/directories, repeated entries are OR\\'d together'"`
	FileMatch   flags.Repeating `subcmd:"file,,'a regular expression to match against filenames, repeated entries are OR\\'d together'"`
	Stats       bool            `subcmd:"stats,false,'calculate statistics on found entries'"`
	TopN        int             `subcmd:"n,50,'number of entries to show for statistics'"`
}

type findCmds struct{}

type orRegexp []*regexp.Regexp

func newOrRegExp(values []string) (orRegexp, error) {
	res := make(orRegexp, len(values))
	for i, v := range values {
		r, err := regexp.Compile(v)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regexp for --match: %v: %v", v, err)
		}
		res[i] = r
	}
	return res, nil
}

func (or orRegexp) match(p string) bool {
	if len(or) == 0 {
		return false
	}
	for _, r := range or {
		if r.MatchString(p) {
			return true
		}
	}
	return false
}

func (fc *findCmds) configure(ff *findFlags) (orPrefixes, orFiles orRegexp, useUID, useGID bool, uid, gid uint32, err error) {
	orPrefixes, err = newOrRegExp(ff.PrefixMatch.Values)
	if err != nil {
		return
	}
	orFiles, err = newOrRegExp(ff.FileMatch.Values)
	if err != nil {
		return
	}
	useUID, useGID = len(ff.User) > 0, len(ff.Group) > 0
	if useUID {
		uid, err = globalUserManager.uidForName(ff.User)
		if err != nil {
			return
		}
	}

	if useGID {
		gid, err = globalUserManager.gidForName(ff.User)
		if err != nil {
			return
		}
	}

}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	ff := values.(*findFlags)
	ctx, cfg, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	orPrefixes, orFiles, useUID, useGID, uid, gid, err := fc.configure(ff)
	if err != nil {
		return err
	}

	sep := cfg.Separator
	calc := cfg.Calculator()
	hasStorabeBytes := calc.String() != "identity"
	sdb := reports.NewAllStats(args[0], hasStorabeBytes, ff.TopN)
	bytes := heap.NewMinMax[int64, string]()

	err = db.Scan(ctx, args[0], func(_ context.Context, k string, v []byte) bool {
		if !strings.HasPrefix(k, args[0]) {
			return false
		}
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal value for %v: %v\n", k, err)
			return false
		}
		puid, pgid := pi.UserGroup()
		if useUID && puid != uid {
			return true
		}
		if useGID && pgid != gid {
			return true
		}

		if orPrefixes.match(k) {
			fmt.Printf("%v\n", k)
			if ff.Stats {
				if err := sdb.Update(k, pi, calc); err != nil {
					fmt.Fprintf(os.Stderr, "failed to compute stats for %v: %v\n", k, err)
				}
			}
			return true
		}

		for _, fi := range pi.InfoList() {
			n := fi.Name()
			if orFiles.match(n) {
				fmt.Printf("---- %v\n", k+sep+n)
				if ff.Stats {
					bytes.PushMaxN(fi.Size(), k+sep+n, ff.TopN)
				}
			}
		}
		return true
	})
	if err != nil {
		return err
	}

	if ff.Stats {
		sdb.Finalize()

		heapFormatter[string]{}.formatTotals(sdb.Prefix, os.Stdout)

		banner(os.Stdout, "=", "Usage by top %v matched refixes\n", ff.TopN)
		heapFormatter[string]{}.formatHeaps(sdb.Prefix, os.Stdout, func(v string) string { return v }, ff.TopN)

		banner(os.Stdout, "=", "Bytes used by matched files\n")
		for bytes.Len() > 0 {
			k, v := bytes.PopMax()
			fmt.Printf("%v %v\n", fmtSize(k), v)
		}
	}

	return nil
}
