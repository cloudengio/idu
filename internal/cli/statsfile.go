// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
	"time"

	"cloudeng.io/cmd/idu/report"
)

// The sources of the statistics in a stats file.
const (
	// StatsFromSummaries denotes stats computed by a pass over all of the
	// summaries, or the file metadata, of the prefix.
	StatsFromSummaries = "summaries"
	// StatsFromStored denotes stats constructed from the recursive totals
	// and rankings maintained by analyze. They do not include the
	// distributed subtree statistics, and the rankings of prefixes for each
	// user and group are limited to topn_per_id entries.
	StatsFromStored = "stored"
)

// StatsFile is the format of the files written by stats compute and read
// by stats view and reports generate.
type StatsFile struct {
	Prefix     string
	Date       time.Time
	Expression string
	Stats      *report.AllStats
	// Source is StatsFromSummaries or StatsFromStored, it is empty for
	// files written before stats could be constructed from the stored
	// state, which were always computed from the summaries.
	Source string
	// PerIDN is the maximum number of entries in the rankings of prefixes
	// for each user and group when Source is StatsFromStored.
	PerIDN int
}

// LoadStats reads a stats file, - denotes os.Stdin.
func LoadStats(filename string) (StatsFile, error) {
	if filename == "-" {
		return DecodeStats(os.Stdin)
	}
	buf, err := os.ReadFile(filename)
	if err != nil {
		return StatsFile{}, err
	}
	return DecodeStats(bytes.NewReader(buf))
}

// DecodeStats decodes a stats file from rd.
func DecodeStats(rd io.Reader) (StatsFile, error) {
	var stats StatsFile
	if err := gob.NewDecoder(rd).Decode(&stats); err != nil {
		return StatsFile{}, err
	}
	return stats, nil
}

// Encode writes sf to w.
func (sf StatsFile) Encode(w io.Writer) error {
	return gob.NewEncoder(w).Encode(sf)
}
