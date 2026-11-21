// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/usernames"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/matcher"
)

// OperandFactory creates an operand for use in an expression, see
// boolexpr.Parser.RegisterOperand.
type OperandFactory func(name, value string) boolexpr.Operand

// ParserOption represents an option to NewParser.
type ParserOption func(o *parserOptions)

type customOperand struct {
	name    string
	factory OperandFactory
}

type parserOptions struct {
	operands []customOperand
}

// WithOperand registers an additional operand with the parser created by
// NewParser. It may also be used to replace one of the default operands
// by specifying the same name. The value passed to the operand's Eval method
// will be either a MatchPrefix or a MatchEntry and operands should return
// false for any other type.
func WithOperand(name string, factory OperandFactory) ParserOption {
	return func(o *parserOptions) {
		o.operands = append(o.operands, customOperand{name: name, factory: factory})
	}
}

// NewParser returns a parser for expressions that can be used with
// NewMatcher. It supports all of the operands provided by
// cloudeng.io/file/matcher as well as user and group operands that accept
// user/group names or IDs, and a hardlink operand that uses fs to determine
// the file referred to. Additional operands may be registered using
// WithOperand.
func NewParser(ctx context.Context, fs filewalk.FS, opts ...ParserOption) *boolexpr.Parser {
	var o parserOptions
	for _, fn := range opts {
		fn(&o)
	}
	parser := matcher.New()
	parser.RegisterOperand("user", func(n, v string) boolexpr.Operand {
		return matcher.NewUser(n, v, func(text string) (file.XAttr, error) {
			return matcher.ParseUsernameOrID(text, usernames.IDM.LookupUser)
		})
	})
	parser.RegisterOperand("group", func(n, v string) boolexpr.Operand {
		return matcher.NewGroup(n, v, func(text string) (file.XAttr, error) {
			return matcher.ParseGroupnameOrID(text, usernames.IDM.LookupGroup)
		})
	})
	parser.RegisterOperand("hardlink", func(n, v string) boolexpr.Operand {
		return newHardlink(ctx, n, v, fs)
	})
	for _, op := range o.operands {
		parser.RegisterOperand(op.name, op.factory)
	}
	return parser
}

// MatcherOption represents an option to NewMatcher.
type MatcherOption func(o *matcherOptions)

// WithExpression specifies the expression to be evaluated. Multiple
// arguments are joined using spaces.
func WithExpression(expr ...string) MatcherOption {
	return func(o *matcherOptions) {
		o.expr = append(o.expr, expr...)
	}
}

// WithEmptyValue specifies the value to be returned when no expression
// is specified.
func WithEmptyValue(v bool) MatcherOption {
	return func(o *matcherOptions) {
		o.emptyValue = v
	}
}

// WithFilewalkFS specifies the filesystem used to construct the names
// and paths of prefixes and entries.
func WithFilewalkFS(fs filewalk.FS) MatcherOption {
	return func(o *matcherOptions) {
		o.fs = fs
	}
}

// WithHardlinkHandling enables incremental detection of hardlinks so as to
// avoid counting the second and subsequent file system entries that
// represent the same file. With this option enabled, IsHardlink will return
// true for any file that has already been seen (based on its device and
// inode numbers).
func WithHardlinkHandling(v bool) MatcherOption {
	return func(o *matcherOptions) {
		o.hardlinks = v
	}
}

type matcherOptions struct {
	expr       []string
	fs         filewalk.FS
	hardlinks  bool
	emptyValue bool
}

// Matcher evaluates a boolean expression against prefixes and the entries
// within them.
type Matcher struct {
	matcherOptions
	expr boolexpr.T
	hl   *hardlinks.Incremental
}

// NewMatcher returns a Matcher for the expression specified via
// WithExpression, parsed using parser. The parser may be nil if no
// expression is specified.
func NewMatcher(parser *boolexpr.Parser, opts ...MatcherOption) (Matcher, error) {
	m := Matcher{}
	for _, fn := range opts {
		fn(&m.matcherOptions)
	}
	if m.hardlinks {
		m.hl = &hardlinks.Incremental{}
	}
	input := strings.TrimSpace(strings.Join(m.matcherOptions.expr, " "))
	if len(input) == 0 {
		return m, nil
	}
	if parser == nil {
		return Matcher{}, fmt.Errorf("failed to parse expression: %v: nil parser", input)
	}
	expr, err := parser.Parse(input)
	if err != nil {
		return Matcher{}, fmt.Errorf("failed to parse expression: %v: %w", input, err)
	}
	m.expr = expr
	return m, nil
}

// AlwaysMatch returns a Matcher that matches all prefixes and entries.
func AlwaysMatch() Matcher {
	m, _ := NewMatcher(nil, WithEmptyValue(true))
	return m
}

// HasExpression reports whether an expression was specified.
func (m Matcher) HasExpression() bool {
	return !m.expr.IsEmpty()
}

// IsHardlink returns true if hardlink handling is enabled and the file
// identified by xattr has been seen before. Files whose inode number is
// zero, ie. unknown, are never hardlinks.
func (m Matcher) IsHardlink(xattr file.XAttr) bool {
	if m.hl == nil || xattr.FileID == 0 {
		return false
	}
	return m.hl.Ref(xattr.Device, xattr.FileID)
}

// Prefix evaluates the expression against prefix.
func (m Matcher) Prefix(prefix string, pi PrefixInfo) bool {
	if m.expr.IsEmpty() {
		return m.emptyValue
	}
	name := prefix
	if m.fs != nil {
		name = m.fs.Base(prefix)
	}
	return m.expr.Eval(MatchPrefix{PrefixInfo: pi, name: name, path: prefix})
}

// Entry evaluates the expression against the entry fi within prefix.
func (m Matcher) Entry(prefix string, pi PrefixInfo, fi file.Info) bool {
	if m.expr.IsEmpty() {
		return m.emptyValue
	}
	path := prefix
	if m.fs != nil {
		path = m.fs.Join(prefix, fi.Name())
	}
	return m.expr.Eval(MatchEntry{pi: pi, fi: fi, path: path})
}

func (m Matcher) String() string {
	ph := "[hardlink handling disabled]"
	if m.hl != nil {
		ph = "[hardlink handling enabled]"
	}
	return fmt.Sprintf("%v: %v (default: %v)", ph, m.expr.String(), m.emptyValue)
}

// MatchEntry is the value that expressions are evaluated against for
// each entry (file or directory) within a prefix.
type MatchEntry struct {
	pi   PrefixInfo
	fi   file.Info
	path string
}

// Info returns the file.Info for the entry.
func (e MatchEntry) Info() file.Info { return e.fi }

// Prefix returns the metadata for the prefix that contains the entry.
func (e MatchEntry) Prefix() PrefixInfo { return e.pi }

// XAttr returns the extended attributes (e.g. user and group) for the entry.
func (e MatchEntry) XAttr() file.XAttr { return e.pi.XAttrInfo(e.fi) }

// Name returns the name of the entry within its prefix.
func (e MatchEntry) Name() string { return e.fi.Name() }

// Path returns the full path name of the entry.
func (e MatchEntry) Path() string { return e.path }

// Type returns the type bits of the entry.
func (e MatchEntry) Type() fs.FileMode { return e.fi.Type() }

// Mode returns the mode of the entry.
func (e MatchEntry) Mode() fs.FileMode { return e.fi.Mode() }

// Size returns the size of the entry in bytes.
func (e MatchEntry) Size() int64 { return e.fi.Size() }

// ModTime returns the modification time of the entry.
func (e MatchEntry) ModTime() time.Time { return e.fi.ModTime() }

// MatchPrefix is the value that expressions are evaluated against
// for each prefix (directory).
type MatchPrefix struct {
	PrefixInfo
	name string
	path string
}

// Name returns the base name of the prefix.
func (p MatchPrefix) Name() string { return p.name }

// Path returns the full path name of the prefix.
func (p MatchPrefix) Path() string { return p.path }

// NumEntries returns the number of entries in the prefix.
func (p MatchPrefix) NumEntries() int64 { return int64(len(p.InfoList())) }
