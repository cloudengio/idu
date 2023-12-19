// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package boolexpr provides a wrapper for cloudeng.io/cmdutil/boolexpr
// for use with idu.
package boolexpr

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/matcher"
)

func NewParser(ctx context.Context, fs filewalk.FS) *boolexpr.Parser {
	parser := matcher.New()

	parser.RegisterOperand("user",
		func(_, v string) boolexpr.Operand {
			return NewUID("user", v, usernames.Manager.UIDForName)
		})

	parser.RegisterOperand("group", func(_, v string) boolexpr.Operand {
		return NewGID("group", v, usernames.Manager.GIDForName)
	})

	parser.RegisterOperand("hardlink", func(n, v string) boolexpr.Operand {
		return NewHardlink(ctx, n, v, fs)
	})

	return parser
}

type Option func(o *options)

func WithEntryExpression(expr ...string) Option {
	return func(o *options) {
		o.entry = append(o.entry, expr...)
	}
}

func WithEmptyEntryValue(v bool) Option {
	return func(o *options) {
		o.emptyEntryValue = v
	}
}

func WithFilewalkFS(fs filewalk.FS) Option {
	return func(o *options) {
		o.fs = fs
	}
}

// WithHardlinkHandling enables incrmental detection of hardlinks so as to
// avoid visiting the second and subsequent file system entries that
// represent the same file. This is primarily useful for avoiding overcounting
// the resources shared by hardlinks. With this option enabled, the matcher's
// Entry method will return false for any file that has already been seen
// (based on its device and inode numbers).
func WithHardlinkHandling(v bool) Option {
	return func(o *options) {
		o.hardlinks = v
	}
}

type options struct {
	entry           []string
	fs              filewalk.FS
	hardlinks       bool
	emptyEntryValue bool
}

func createExpr(p *boolexpr.Parser, args []string) (boolexpr.T, bool, error) {
	input := strings.Join(args, " ")
	input = strings.TrimSpace(input)
	if len(input) == 0 {
		// If no expression is specified, then always return true.
		return boolexpr.T{}, false, nil
	}
	expr, err := p.Parse(input)
	if err != nil {
		return boolexpr.T{}, false, fmt.Errorf("failed to parse expression: %v: %v\n", input, err)
	}
	return expr, true, nil
}

func CreateMatcher(parser *boolexpr.Parser, opts ...Option) (Matcher, error) {
	m := match{}
	for _, fn := range opts {
		fn(&m.options)
	}
	if m.hardlinks {
		m.hl = &hardlinks.Incremental{}
	}
	var err error
	m.expr, m.exprSet, err = createExpr(parser, m.entry)
	if err != nil {
		return match{}, err
	}
	return m, nil
}

type match struct {
	options
	exprSet bool
	expr    boolexpr.T
	hl      *hardlinks.Incremental
}

func (m match) IsHardlink(prefix string, info *prefixinfo.T, fi file.Info) bool {
	if m.hl == nil {
		return false
	}
	xattr := info.XAttrInfo(fi)
	return m.hl.Ref(xattr.Device, xattr.FileID)
}

func (m match) Prefix(prefix string, pi *prefixinfo.T) bool {
	if !m.exprSet {
		return m.emptyEntryValue
	}
	name := prefix
	if m.fs != nil {
		name = m.fs.Base(prefix)
	}
	return m.expr.Eval(prefixWithName{T: pi, name: name, path: prefix})
}

func (m match) Entry(prefix string, pi *prefixinfo.T, fi file.Info) bool {
	if !m.exprSet {
		return m.emptyEntryValue
	}
	path := prefix
	if m.fs != nil {
		path = m.fs.Join(prefix, fi.Name())
	}
	return m.expr.Eval(entryWithXattr{pi: pi, fi: fi, path: path})
}

func (m match) String() string {
	ph := "[hardlink handling disabled]:"
	if m.hl != nil {
		ph = "[hardlink handling enabled]:"
	}
	return fmt.Sprintf("%v: pentry: %v (default: %v)", ph, m.expr.String(), m.emptyEntryValue)
}

type entryWithXattr struct {
	pi   *prefixinfo.T
	fi   file.Info
	path string
}

func (w entryWithXattr) XAttr() filewalk.XAttr {
	return w.pi.XAttrInfo(w.fi)
}

func (w entryWithXattr) Name() string {
	return w.fi.Name()
}

func (w entryWithXattr) Path() string {
	return w.path
}

func (w entryWithXattr) Type() fs.FileMode {
	return w.fi.Type()
}

func (w entryWithXattr) Mode() fs.FileMode {
	return w.fi.Mode()
}

type prefixWithName struct {
	*prefixinfo.T
	name string
	path string
}

func (pi prefixWithName) Name() string {
	return pi.name
}

func (pi prefixWithName) Path() string {
	return pi.path
}

func (pi prefixWithName) NumEntries() int64 {
	return int64(len(pi.T.InfoList()))
}

type AlwaysMatch struct{}

func (AlwaysMatch) IsHardlink(prefix string, info *prefixinfo.T, fi file.Info) bool {
	return false
}

func (AlwaysMatch) Entry(prefix string, info *prefixinfo.T, fi file.Info) bool {
	return true
}

func (AlwaysMatch) Prefix(prefix string, info *prefixinfo.T) bool {
	return true
}

func (AlwaysMatch) String() string {
	return "always match"
}

type Matcher interface {
	IsHardlink(prefix string, info *prefixinfo.T, fi file.Info) bool
	Entry(prefix string, info *prefixinfo.T, fi file.Info) bool
	Prefix(prefix string, info *prefixinfo.T) bool
	String() string
}
