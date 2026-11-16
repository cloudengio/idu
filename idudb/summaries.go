// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"cloudeng.io/cmd/idu/stats"
)

const metaKeySummaries = "summaries/v1"

const (
	hardlinksSpecSuffix = ";hardlinks"
	// invalidSummariesSpec is stored when a summary could not be written
	// so that the next analyze rewrites them all.
	invalidSummariesSpec = "invalid"
)

// summaryFormatSuffix identifies the encoding of the summaries so that
// changing it causes the existing summaries to be treated as stale and
// to be rewritten by the next analyze.
var summaryFormatSuffix = fmt.Sprintf(";v%d", stats.PrefixSummaryVersion)

// summarySpec returns the specification for summaries computed with
// calcID that record hardlinks if hardlinks is true.
func summarySpec(calcID string, hardlinks bool) string {
	spec := calcID + summaryFormatSuffix
	if hardlinks {
		spec += hardlinksSpecSuffix
	}
	return spec
}

// ErrStaleSummaries is returned when the precomputed summaries are
// incomplete or were computed using a different configuration.
var ErrStaleSummaries = errors.New("summaries are incomplete or were computed with a different configuration (layout or hardlink settings): rerun analyze for the entire prefix")

// initSummaryMeta returns true if the existing summaries were computed
// with a specification other than spec and hence need to be rewritten.
// New rejects legacy databases, so a missing metadata entry
// denotes a new, empty, database. When a rewrite is required, the stored
// specification is invalidated immediately so partial or interrupted scans
// do not leave mixed summaries marked as valid under the prior spec.
func initSummaryMeta(ctx context.Context, db Database, spec string) (bool, error) {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return false, err
	}
	if metaBuf.Len() == 0 {
		return false, db.SetMeta(ctx, metaKeySummaries, []byte(spec))
	}
	if metaBuf.String() == spec {
		return false, nil
	}
	if metaBuf.String() != invalidSummariesSpec {
		if err := db.SetMeta(ctx, metaKeySummaries, []byte(invalidSummariesSpec)); err != nil {
			return false, err
		}
	}
	return true, nil
}

// CheckSummaries returns ErrStaleSummaries unless db contains complete
// precomputed summaries computed with calcID that also record hardlinks
// if needLinks is true.
func CheckSummaries(ctx context.Context, db Reader, calcID string, needLinks bool) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	switch metaBuf.String() {
	case summarySpec(calcID, true):
		return nil
	case summarySpec(calcID, false):
		if !needLinks {
			return nil
		}
	}
	return ErrStaleSummaries
}

// CheckSummaryKeys returns ErrStaleSummaries if a summary could not be
// written and hence the summary keys may not include every prefix.
func CheckSummaryKeys(ctx context.Context, db Reader) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	if metaBuf.String() == invalidSummariesSpec {
		return ErrStaleSummaries
	}
	return nil
}
