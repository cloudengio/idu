// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package find implements idu's commands that scan the database for
// prefixes and files that match flexible criteria, ie. find.
package find

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

// Flags are the flags for the find command.
type Flags struct {
	cli.FSFlag
	Long   bool            `subcmd:"l,false,'show long listing for each result'"`
	Prefix flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}

// Commands implements the commands that scan the database for prefixes
// and files that match flexible criteria, ie. find.
type Commands struct {
	env *cli.Env
}

// New returns the Commands that use env.
func New(env *cli.Env) *Commands {
	return &Commands{env: env}
}

// Find implements the find command, values must be a *Flags.
func (fc *Commands) Find(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return fc.findFS(ctx, fs, values.(*Flags), args)
}

func printPrefix(p *summary.Prefix, long bool) {
	if long {
		info := p.Info()
		fmt.Printf("%s uid: %v gid: %v\n", fs.FormatFileInfo(info), info.XAttr.UID, info.XAttr.GID)
	} else {
		fmt.Printf("%v\n", p.Path())
	}
}

func printEntry(p *summary.Prefix, e summary.Info, long bool, sep string) {
	if long {
		fmt.Printf("    %s uid: %v gid: %v\n", fs.FormatFileInfo(e), e.XAttr.UID, e.XAttr.GID)
	} else {
		n := strings.TrimSuffix(p.Path(), sep) + sep + e.Name()
		fmt.Printf("%v\n", n)
	}
}

func (fc *Commands) findFS(ctx context.Context, fwfs filewalk.FS, ff *Flags, args []string) error {

	parser := stats.NewParser(ctx, fwfs)

	prefixMatch, err := stats.NewMatcher(parser,
		stats.WithEmptyValue(true),
		stats.WithFilewalkFS(fwfs),
		stats.WithExpression(ff.Prefix.Values...))
	if err != nil {
		return err
	}

	match, err := stats.NewMatcher(parser,
		stats.WithEmptyValue(true),
		stats.WithFilewalkFS(fwfs),
		stats.WithExpression(args[1:]...))
	if err != nil {
		return err
	}

	ctx, cfg, target, db, err := fc.env.ForFS(ff.FS).OpenPrefixReadOnly(ctx, args[0])
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	sep := cfg.SeparatorOrDefault()
	for p, err := range summary.New(db, cfg).Prefixes(ctx, target) {
		if err != nil {
			return err
		}
		if !p.Match(prefixMatch) {
			continue
		}
		if p.Match(match) {
			printPrefix(p, ff.Long)
		}
		for _, e := range p.Entries() {
			if e.IsDir() {
				continue
			}
			if p.MatchEntry(match, e) {
				printEntry(p, e, ff.Long, sep)
			}
		}
	}
	return nil
}
