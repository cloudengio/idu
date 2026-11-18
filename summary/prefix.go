// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package summary

import (
	"bytes"
	"context"
	"fmt"
	"iter"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

var _ stats.PrefixInfo = (*prefixinfo.T)(nil)

// Info describes a prefix or one of its entries (a file or a prefix).
// It implements fs.FileInfo, note that its Sys method returns an
// implementation specific value, use XAttr instead.
type Info struct {
	file.Info
	// XAttr contains the user, group, device, inode etc. information.
	XAttr file.XAttr
}

// Prefix is a read-only view of all of the metadata recorded for a single
// prefix (directory) by the most recent analysis, ie. the prefix itself and
// all of its files and immediate child prefixes.
type Prefix struct {
	path string
	pi   prefixinfo.T
}

// Decode decodes val, the metadata stored for path as returned by the Get,
// GetBatch or Scan methods of idudb.Reader, into p. It copies everything
// that it needs from val. It is intended for use by callers that read the
// metadata for many prefixes directly, eg. concurrently, see Reader.Prefix
// and Reader.Prefixes for simpler alternatives.
func (p *Prefix) Decode(path string, val []byte) error {
	p.path = path
	if err := p.pi.UnmarshalBinary(val); err != nil {
		return fmt.Errorf("%v: %w", path, err)
	}
	return nil
}

// Path returns the path of the prefix.
func (p *Prefix) Path() string {
	return p.path
}

// Info returns information on the prefix itself. Since a prefix's name is
// implied by its path, the returned Info's Name is the prefix's full path.
func (p *Prefix) Info() Info {
	return Info{
		Info:  file.NewInfo(p.path, p.pi.Size(), p.pi.Mode(), p.pi.ModTime(), nil),
		XAttr: p.pi.XAttr(),
	}
}

// Entries returns the files and immediate child prefixes, ie. those for
// which IsDir is true, of the prefix in the order in which they were
// recorded.
func (p *Prefix) Entries() []Info {
	entries := p.pi.InfoList()
	infos := make([]Info, len(entries))
	for i, fi := range entries {
		infos[i] = Info{Info: fi, XAttr: p.pi.XAttrInfo(fi)}
	}
	return infos
}

// Match reports whether the prefix itself matches m.
func (p *Prefix) Match(m stats.Matcher) bool {
	return m.Prefix(p.path, &p.pi)
}

// MatchEntry reports whether e, which must have been returned by Entries,
// matches m.
func (p *Prefix) MatchEntry(m stats.Matcher, e Info) bool {
	return m.Entry(p.path, &p.pi, e.Info)
}

// Totals returns the totals for the prefix and its files, overall and per
// user and group, computed using calc for those entries that match m. Unlike
// the precomputed summaries, it can be used with arbitrary calculators and
// matchers.
func (p *Prefix) Totals(calc diskusage.Calculator, m stats.Matcher) (totals stats.Totals, perUser, perGroup stats.PerIDTotals) {
	return stats.ComputeTotals(p.path, &p.pi, calc, m)
}

// Prefix returns the metadata recorded for path, if any.
func (r *Reader) Prefix(ctx context.Context, path string) (*Prefix, bool, error) {
	if r.err != nil {
		return nil, false, r.err
	}
	path = stats.NormalizePath(r.sep, path)
	var buf bytes.Buffer
	if err := r.db.Get(ctx, path, &buf); err != nil {
		return nil, false, err
	}
	if buf.Len() == 0 {
		return nil, false, nil
	}
	p := &Prefix{}
	if err := p.Decode(path, buf.Bytes()); err != nil {
		return nil, false, err
	}
	return p, true, nil
}

// Prefixes returns an iterator over the metadata recorded for path and all
// of the prefixes beneath it, in lexicographic order of their paths. Note
// that this is not necessarily depth-first order, eg. /a-b sorts between
// /a and /a/b, but /a-b itself is not visited when path is /a. Iteration
// stops at the first error, which is returned as the final value of the
// iterator.
func (r *Reader) Prefixes(ctx context.Context, path string) iter.Seq2[*Prefix, error] {
	return func(yield func(*Prefix, error) bool) {
		if r.err != nil {
			yield(nil, r.err)
			return
		}
		path := stats.NormalizePath(r.sep, path)
		stopped := false
		var decodeErr error
		err := r.db.Scan(ctx, path, func(_ context.Context, key string, val []byte) bool {
			inSubtree, stop := internal.SubtreeMatch(key, path, r.sep)
			if stop {
				return false
			}
			if !inSubtree {
				return true
			}
			p := &Prefix{}
			if decodeErr = p.Decode(key, val); decodeErr != nil {
				return false
			}
			if !yield(p, nil) {
				stopped = true
				return false
			}
			return true
		})
		if stopped {
			return
		}
		if err == nil {
			err = decodeErr
		}
		if err != nil {
			yield(nil, err)
		}
	}
}
