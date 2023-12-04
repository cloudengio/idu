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
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/matcher"
)

type findFlags struct {
	Stats    bool `subcmd:"stats,false,'calculate statistics on found entries'"`
	TopN     int  `subcmd:"n,50,'number of entries to show for statistics'"`
	Long     bool `subcmd:"long,false,'show long listing for each result'"`
	Document bool `subcmd:"document,false,'show documentation for find expressions'"`
}

type findCmds struct{}

type fileWithID struct {
	file.Info
	uid, gid uint32
}

func (fwid fileWithID) UserGroup() (uint32, uint32) {
	return fwid.uid, fwid.gid
}

func (fc *findCmds) createParser() *boolexpr.Parser {
	parser := matcher.New()
	prefixinfo.RegisterOperands(parser, globalUserManager.uidForName, globalUserManager.gidForName)
	return parser
}

func (fc *findCmds) document() {
	p := fc.createParser()
	fmt.Printf("find accepts boolean expressions using || && and ( and ) to combine any of the following operands:\n\n")
	for _, op := range p.ListOperands() {
		fmt.Printf("  %v\n", op.Document())
	}
	fmt.Printf("\nNote that directories are evaluated both using their full path name as well as their name within a parent, whereas files use evaluated just using their name within a directory.\n")
}

func (fc *findCmds) createExpr(args []string) (boolexpr.T, error) {
	input := strings.Join(args, " ")
	expr, err := fc.createParser().Parse(input)
	if err != nil {
		return boolexpr.T{}, fmt.Errorf("failed to parse expresion: %v: %v\n", input, err)
	}
	return expr, nil
}

type findHandler struct {
	sep   string
	calc  diskusage.Calculator
	stats *reports.AllStats
	expr  boolexpr.T
	long  bool
	topN  int
	bytes *heap.MinMax[int64, string]
}

func (fh *findHandler) handlePrefix(k string, pi prefixinfo.T) {
	named := prefixinfo.NewNamed(k, pi)
	if fh.expr.Eval(named) {
		if fh.stats != nil {
			if err := fh.stats.Update(k, pi, fh.calc); err != nil {
				fmt.Fprintf(os.Stderr, "failed to compute stats for %v: %v\n", k, err)
			}
			return
		}
		if fh.long {
			fmt.Println(fs.FormatFileInfo(internal.PrefixInfoAsFSInfo(pi, k)))
		} else {
			fmt.Printf("%v/\n", k)
		}
	}

	for _, fi := range pi.InfoList() {
		uid, gid := pi.UserGroupInfo(fi)
		fid := fileWithID{Info: fi, uid: uid, gid: gid}
		n := fid.Name()
		if fh.expr.Eval(fid) {
			if fh.stats != nil {
				fh.bytes.PushMaxN(fi.Size(), k+fh.sep+n, fh.topN)
				continue
			}
			if fh.long {
				fmt.Println("    ", fs.FormatFileInfo(fi))
			} else {
				fmt.Printf("%v\n", k+fh.sep+n)
			}
		}
	}
}

func (fc *findCmds) find(ctx context.Context, values interface{}, args []string) error {
	ff := values.(*findFlags)
	if ff.Document {
		fc.document()
		return nil
	}

	expr, err := fc.createExpr(args[1:])
	if err != nil {
		return err
	}

	ctx, cfg, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	fh := &findHandler{
		expr:  expr,
		long:  ff.Long,
		sep:   cfg.Separator,
		calc:  cfg.Calculator(),
		topN:  ff.TopN,
		bytes: heap.NewMinMax[int64, string](),
	}
	hasStorabeBytes := fh.calc.String() != "identity"
	if ff.Stats {
		fh.stats = reports.NewAllStats(args[0], hasStorabeBytes, ff.TopN)
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

		fh.handlePrefix(k, pi)

		return true
	})
	if err != nil {
		return err
	}
	if ff.Stats {
		fc.stats(fh.stats, fh.bytes, ff.TopN)
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
