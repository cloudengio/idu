// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"

	"cloudeng.io/cmd/idu/report"
)

// The sources of the statistics in a stats file.
const (
	// FromSummariesSource denotes stats computed by a pass over all of the
	// summaries, or the file metadata, of the prefix.
	FromSummariesSource = "summaries"
	// FromStoredSource denotes stats constructed from the recursive totals
	// and rankings maintained by analyze. They do not include the
	// distributed subtree statistics, and the rankings of prefixes for each
	// user and group are limited to topn_per_id entries.
	FromStoredSource = "stored"
)

// File is the format of a snapshot, ie. the files written by analyze
// --snapshot and stats compute, and read by stats view and reports
// generate.
type File struct {
	Prefix     string
	Date       time.Time
	Expression string
	Stats      *report.AllStats
	// Source is FromSummariesSource or FromStoredSource, it is empty for
	// files written before stats could be constructed from the stored
	// state, which were always computed from the summaries.
	Source string
	// Whole is true if the stats are for the entire filesystem, ie. its
	// root, without a filter expression, and hence may be used in place
	// of the database by stats view and reports generate. It is false for
	// files written before it was introduced.
	Whole bool
	// PerIDN is the maximum number of entries in the rankings of prefixes
	// for each user and group when Source is FromStoredSource.
	PerIDN int
}

// Load reads a stats file, - denotes os.Stdin.
func Load(filename string) (File, error) {
	if filename == "-" {
		return Decode(os.Stdin)
	}
	buf, err := os.ReadFile(filename)
	if err != nil {
		return File{}, err
	}
	return Decode(bytes.NewReader(buf))
}

// Decode decodes a stats file from rd.
func Decode(rd io.Reader) (File, error) {
	var stats File
	if err := gob.NewDecoder(rd).Decode(&stats); err != nil {
		return File{}, err
	}
	return stats, nil
}

// Encode writes sf to w.
func (sf File) Encode(w io.Writer) error {
	return gob.NewEncoder(w).Encode(sf)
}

// DescribeSource describes the source of the stats in sf.
func (sf File) DescribeSource() string {
	switch sf.Source {
	case FromStoredSource:
		return fmt.Sprintf("the rankings and recursive totals maintained by analyze (at most %v entries per user and group)", sf.PerIDN)
	case "", FromSummariesSource:
		return "the summaries"
	}
	return sf.Source
}
