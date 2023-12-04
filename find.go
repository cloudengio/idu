// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/file"
	"cloudeng.io/file/matcher"
)

type findFlags struct {
	Stats bool `subcmd:"stats,false,'calculate statistics on found entries'"`
	TopN  int  `subcmd:"n,50,'number of entries to show for statistics'"`
	Long  bool `subcmd:"long,false,'show long listing for each result'"`
}

type findCmds struct{}

type fileWithID struct {
	file.Info
	uid, gid uint32
}

func (fwid fileWithID) UserGroup() (uint32, uint32) {
	return fwid.uid, fwid.gid
}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	ff := values.(*findFlags)
	ctx, cfg, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	sep := cfg.Separator
	calc := cfg.Calculator()
	hasStorabeBytes := calc.String() != "identity"
	sdb := reports.NewAllStats(args[0], hasStorabeBytes, ff.TopN)
	bytes := heap.NewMinMax[int64, string]()

	parser := matcher.New()
	prefixinfo.RegisterOperands(parser, globalUserManager.uidForName, globalUserManager.gidForName)
	expr, err := parser.Parse(strings.Join(args[1:], " "))
	if err != nil {
		return err
	}

	err = db.Scan(ctx, args[0], func(_ context.Context, k string, v []byte) bool {
		if !strings.HasPrefix(k, args[0]) {
			return false
		}
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal value for %v: %v\n", k, err)
			return false
		}

		named := prefixinfo.NewNamed(k, pi)
		if expr.Eval(named) {
			if ff.Stats {
				if err := sdb.Update(k, pi, calc); err != nil {
					fmt.Fprintf(os.Stderr, "failed to compute stats for %v: %v\n", k, err)
				}
			} else {
				if ff.Long {
					fmt.Println(fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, k)))
				} else {
					fmt.Printf("%v/\n", k)
				}
			}
		}

		for _, fi := range pi.InfoList() {
			uid, gid := pi.UserGroupInfo(fi)
			fid := fileWithID{Info: fi, uid: uid, gid: gid}
			n := fid.Name()
			if expr.Eval(fid) {
				if ff.Stats {
					bytes.PushMaxN(fi.Size(), k+sep+n, ff.TopN)
					continue
				}
				if ff.Long {
					fmt.Println("    ", fs.FormatFileInfo(fi))
				} else {
					fmt.Printf("%v\n", k+sep+n)
				}

			}
		}
		return true
	})
	if err != nil {
		return err
	}

	if ff.Stats {
		fc.stats(sdb, bytes, ff.TopN)
	}

	return nil
}

func (fc *findCmds) stats(sdb *reports.AllStats, bytes *heap.MinMax[int64, string], topN int) {
	sdb.Finalize()

	heapFormatter[string]{}.formatTotals(sdb.Prefix, os.Stdout)

	banner(os.Stdout, "=", "Usage by top %v matched prefixes\n", topN)
	heapFormatter[string]{}.formatHeaps(sdb.Prefix, os.Stdout, func(v string) string { return v }, topN)

	banner(os.Stdout, "=", "Bytes used by matched files\n")
	for bytes.Len() > 0 {
		k, v := bytes.PopMax()
		fmt.Printf("%v %v\n", fmtSize(k), v)
	}
}
