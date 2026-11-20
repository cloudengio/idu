// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/stats"
)

const metaKeySummaries = "summaries/v1"

const (
	// hardlinksSpecSuffix denotes summaries that record the files that may
	// be hardlinks. It was previously used for summaries that record every
	// file, which are a superset of those, and hence remain valid.
	hardlinksSpecSuffix = ";hardlinks"
	// allFilesSpecSuffix denotes summaries that record every file.
	allFilesSpecSuffix = ";hardlinks-all"
	// recordAllSpecSep precedes the identifier of the prefixes whose
	// summaries record every file when only the files that may be
	// hardlinks are otherwise recorded, see WithRecordAllFiles.
	recordAllSpecSep = ";record-all="
	// invalidSummariesSpec is stored when a summary could not be written
	// so that the next analyze rewrites them all.
	invalidSummariesSpec = "invalid"
)

// summaryFormatSuffix identifies the encoding of the summaries so that
// changing it causes the existing summaries to be treated as stale and
// to be rewritten by the next analyze.
var summaryFormatSuffix = fmt.Sprintf(";v%d", stats.PrefixSummaryVersion)

// summarySpec returns the specification for summaries computed with
// calcID that record the files specified by rec.
func summarySpec(calcID string, rec stats.HardlinkRecording) string {
	spec := calcID + summaryFormatSuffix
	switch rec {
	case stats.RecordLinkedFiles:
		spec += hardlinksSpecSuffix
	case stats.RecordAllFiles:
		spec += allFilesSpecSuffix
	}
	return spec
}

// withRecordAll returns spec, a specification for summaries that record
// the files that may be hardlinks, extended to identify the prefixes whose
// summaries record every file.
func withRecordAll(spec, recordAllID string) string {
	if recordAllID == "" {
		return spec
	}
	return spec + recordAllSpecSep + recordAllID
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
// precomputed summaries computed with calcID that also record hardlinks,
// either the files that may be linked or all files, if needLinks is true.
func CheckSummaries(ctx context.Context, db Reader, calcID string, needLinks bool) error {
	rec := stats.RecordNoFiles
	if needLinks {
		rec = stats.RecordLinkedFiles
	}
	return CheckSummaryRecording(ctx, db, calcID, rec)
}

// CheckSummaryRecording returns ErrStaleSummaries unless db contains complete
// precomputed summaries computed with calcID that satisfy the hardlink
// recording requirement rec. RecordAllFiles satisfies RecordLinkedFiles, and
// both satisfy RecordNoFiles.
func CheckSummaryRecording(ctx context.Context, db Reader, calcID string, rec stats.HardlinkRecording) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	stored := metaBuf.String()
	if base, _, ok := strings.Cut(stored, recordAllSpecSep); ok && rec != stats.RecordAllFiles {
		// Recording every file for some prefixes satisfies the
		// requirements for recording those that may be hardlinks.
		stored = base
	}
	switch rec {
	case stats.RecordAllFiles:
		if stored == summarySpec(calcID, stats.RecordAllFiles) {
			return nil
		}
	case stats.RecordLinkedFiles:
		if stored == summarySpec(calcID, stats.RecordLinkedFiles) || stored == summarySpec(calcID, stats.RecordAllFiles) {
			return nil
		}
	case stats.RecordNoFiles:
		switch stored {
		case summarySpec(calcID, stats.RecordNoFiles),
			summarySpec(calcID, stats.RecordLinkedFiles),
			summarySpec(calcID, stats.RecordAllFiles):
			return nil
		}
	}
	return fmt.Errorf("%w: they %v, but %v are required", ErrStaleSummaries,
		describeSpec(metaBuf.String()), describeRequirement(calcID, rec))
}

// describeSpec describes the summaries recorded by spec, as stored in the
// database's metadata.
func describeSpec(spec string) string {
	switch spec {
	case "":
		return "have not been computed"
	case invalidSummariesSpec:
		return "are incomplete since a previous analysis did not finish writing them"
	}
	calc, rest, _ := strings.Cut(spec, ";")
	parts := strings.Split(rest, ";")
	version, recording := "", "no hardlink information"
	for _, p := range parts {
		switch {
		case strings.HasPrefix(p, "v"):
			version = p
		case p == strings.TrimPrefix(hardlinksSpecSuffix, ";"):
			recording = "the files that may be hardlinks"
		case p == strings.TrimPrefix(allFilesSpecSuffix, ";"):
			recording = "every file"
		case strings.HasPrefix(p, strings.TrimPrefix(recordAllSpecSep, ";")):
			recording += ", and every file for the prefixes in hardlinks_record_all"
		}
	}
	return fmt.Sprintf("were computed with the %q calculator, format %v, recording %v", calc, version, recording)
}

// describeRequirement describes the summaries required by
// CheckSummaryRecording.
func describeRequirement(calcID string, rec stats.HardlinkRecording) string {
	recording := map[stats.HardlinkRecording]string{
		stats.RecordNoFiles:     "any hardlink information",
		stats.RecordLinkedFiles: "at least the files that may be hardlinks",
		stats.RecordAllFiles:    "every file",
	}[rec]
	return fmt.Sprintf("summaries computed with the %q calculator, format %v, recording %v", calcID, strings.TrimPrefix(summaryFormatSuffix, ";"), recording)
}

// CheckSummaryKeys returns ErrStaleSummaries if a summary could not be
// written and hence the summary keys may not include every prefix.
func CheckSummaryKeys(ctx context.Context, db Reader) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	if metaBuf.String() == invalidSummariesSpec {
		return fmt.Errorf("%w: they %v", ErrStaleSummaries, describeSpec(invalidSummariesSpec))
	}
	return nil
}
