// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package boolexpr provides a wrapper for cloudeng.io/cmdutil/boolexpr
// for use with idu.
package boolexpr

import (
	"fmt"
	"io/fs"
	"strings"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/matcher"
)

func NewParser(fs fs.FS) *boolexpr.Parser {
	parser := matcher.New()

	parser.RegisterOperand("user",
		func(_, v string) boolexpr.Operand {
			return NewUID("user", v, usernames.Manager.UIDForName)
		})

	parser.RegisterOperand("group", func(_, v string) boolexpr.Operand {
		return NewGID("group", v, usernames.Manager.GIDForName)
	})

	parser.RegisterOperand("hardlink", func(n, v string) boolexpr.Operand {
		return NewHardlink(n, v, fs)
	})

	return parser
}

type Option func(o *options)

func WithEntryExpression(expr ...string) Option {
	return func(o *options) {
		o.entry = append(o.entry, expr...)
	}
}

func WithPrefixExpression(expr ...string) Option {
	return func(o *options) {
		o.prefix = append(o.prefix, expr...)
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
	entry, prefix []string
	hardlinks     bool
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
	options := &options{}
	for _, fn := range opts {
		fn(options)
	}
	m := match{}
	if options.hardlinks {
		m.hl = &hardlinks.Incremental{}
	}

	var err error
	m.entryExpr, m.entrySet, err = createExpr(parser, options.entry)
	if err != nil {
		return match{}, err
	}
	m.prefixExpr, m.prefixSet, err = createExpr(parser, options.prefix)
	if err != nil {
		return match{}, err
	}
	return m, nil
}

type match struct {
	prefixSet, entrySet   bool
	prefixExpr, entryExpr boolexpr.T
	hl                    *hardlinks.Incremental
}

func (m match) IsHardlink(prefix string, info *prefixinfo.T, fi file.Info) bool {
	if m.hl == nil {
		return false
	}
	_, _, dev, ino, _ := info.SysInfo(fi)
	return m.hl.Ref(dev, ino)
}

func (m match) IsPrefixSet() bool {
	return m.prefixSet
}

func (m match) Prefix(prefix string, pi *prefixinfo.T) bool {
	return m.prefixExpr.Eval(prefixinfo.NewNamed(prefix, pi))
}

func (m match) Entry(prefix string, pi *prefixinfo.T, fi file.Info) bool {
	if !m.entrySet {
		return true
	}
	return m.entryExpr.Eval(withsys{pi, fi})
}

func (m match) String() string {
	ph := "[hardlink handling disabled]:"
	if m.hl != nil {
		ph = "[hardlink handling enabled]:"
	}
	return fmt.Sprintf("%v: prefix: %v, entry: %v", ph, m.prefixExpr.String(), m.entryExpr.String())
}

type withsys struct {
	pi *prefixinfo.T
	fi file.Info
}

func (w withsys) UserGroup() (uid, gid uint32) {
	uid, gid, _, _, _ = w.pi.SysInfo(w.fi)
	return
}

func (w withsys) DevIno() (dev, ino uint64) {
	_, _, dev, ino, _ = w.pi.SysInfo(w.fi)
	return
}

func (w withsys) Name() string {
	return w.fi.Name()
}

func (w withsys) Type() fs.FileMode {
	return w.fi.Type()
}

func (w withsys) Mode() fs.FileMode {
	return w.fi.Mode()
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

func (AlwaysMatch) IsPrefixSet() bool {
	return false
}

func (AlwaysMatch) String() string {
	return "always match"
}

type Matcher interface {
	IsHardlink(prefix string, info *prefixinfo.T, fi file.Info) bool
	Entry(prefix string, info *prefixinfo.T, fi file.Info) bool
	Prefix(prefix string, info *prefixinfo.T) bool
	IsPrefixSet() bool
	String() string
}
