// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"sync"

	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/sync/syncsort"
)

const (
	// prefixBatchSize is the number of prefixes read from the database in
	// a single transaction.
	prefixBatchSize = 256
	// prefixBatchesPerReader bounds the number of batches that may be
	// outstanding, i.e. being read or waiting to be consumed in order,
	// per reader.
	prefixBatchesPerReader = 4
)

// prefixEntry holds the metadata read for a single prefix.
type prefixEntry struct {
	summaryKey string
	prefix     string
	pi         prefixinfo.T
	missing    bool  // no metadata is stored for this prefix.
	decodeErr  error // the metadata could not be decoded.
}

type prefixBatch struct {
	entries []prefixEntry
	err     error // a database error, which is fatal.
}

func (b *prefixBatch) read(ctx context.Context, db database.DB, prefixes []string) {
	b.err = db.GetBatch(ctx, prefixes, func(i int, val []byte) error {
		e := &b.entries[i]
		if val == nil {
			e.missing = true
			return nil
		}
		// UnmarshalBinary copies everything it needs from val.
		e.decodeErr = e.pi.UnmarshalBinary(val)
		return nil
	})
}

type prefixJob struct {
	item     syncsort.Item[*prefixBatch]
	prefixes []string
}

// prefixReader implements readPrefixesInOrder: a single goroutine scans the
// summary keys and creates batches of prefixes, which are read concurrently
// by a pool of reader goroutines and then reordered by a syncsort.Sequencer.
type prefixReader struct {
	db                         database.DB
	sep, exactKey, childPrefix string
	// tokens bounds the number of outstanding batches, including those
	// that have been read out of order and are held by the sequencer.
	tokens  chan struct{}
	jobs    chan prefixJob
	results chan syncsort.Item[*prefixBatch]
	seq     *syncsort.Sequencer[*prefixBatch]
	scanErr chan error
	wg      sync.WaitGroup
}

// readPrefixesInOrder calls visit with the metadata for every prefix whose
// summary key is exactKey or matches childPrefix, in summary key, and hence
// DFS pre-order. The metadata is read and decoded concurrently by the
// specified number of readers whilst visit is always called from the
// calling goroutine. Memory usage is bounded by the number of readers.
func readPrefixesInOrder(ctx context.Context, db database.DB, sep, exactKey, childPrefix string, readers int, visit func(*prefixEntry)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	readers = max(readers, 1)
	results := make(chan syncsort.Item[*prefixBatch], readers)
	pr := &prefixReader{
		db:          db,
		sep:         sep,
		exactKey:    exactKey,
		childPrefix: childPrefix,
		tokens:      make(chan struct{}, readers*prefixBatchesPerReader),
		jobs:        make(chan prefixJob, readers),
		results:     results,
		seq:         syncsort.NewSequencer(ctx, results),
		scanErr:     make(chan error, 1),
	}
	go pr.scan(ctx)
	for range readers {
		pr.wg.Go(func() { pr.read(ctx) })
	}
	go func() {
		pr.wg.Wait()
		close(pr.results)
	}()
	err := pr.consume(visit)
	// Stop the scanner and readers before waiting for them to finish.
	cancel()
	return pr.wait(err)
}

func (pr *prefixReader) scan(ctx context.Context) {
	defer close(pr.jobs)
	pr.scanErr <- scanBatches(ctx, pr.db, pr.sep, pr.exactKey, pr.childPrefix, func(b *prefixBatch, prefixes []string) bool {
		select {
		case pr.tokens <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		// The order of calls to NextItem determines the order in which
		// the batches are scanned.
		select {
		case pr.jobs <- prefixJob{item: pr.seq.NextItem(b), prefixes: prefixes}:
			return true
		case <-ctx.Done():
			return false
		}
	})
}

func (pr *prefixReader) read(ctx context.Context) {
	for j := range pr.jobs {
		j.item.V.read(ctx, pr.db, j.prefixes)
		select {
		case pr.results <- j.item:
		case <-ctx.Done():
			return
		}
	}
}

func (pr *prefixReader) consume(visit func(*prefixEntry)) error {
	for pr.seq.Scan() {
		// Scan returns true, with a nil item, when the context is canceled.
		if pr.seq.Err() != nil {
			break
		}
		b := pr.seq.Item()
		<-pr.tokens
		if b.err != nil {
			return b.err
		}
		for i := range b.entries {
			visit(&b.entries[i])
		}
	}
	return pr.seq.Err()
}

// wait must be called after the context passed to scan and read is canceled.
func (pr *prefixReader) wait(err error) error {
	// Drain the sequencer so that its goroutine exits.
	for pr.seq.Scan() {
	}
	pr.wg.Wait()
	if serr := <-pr.scanErr; err == nil && serr != nil && !errors.Is(serr, context.Canceled) {
		err = serr
	}
	return err
}

// scanBatches calls send with batches of the prefixes whose summary key is
// exactKey or matches childPrefix, in summary key order, and the prefixes
// that they correspond to.
func scanBatches(ctx context.Context, db database.DB, sep, exactKey, childPrefix string, send func(*prefixBatch, []string) bool) error {
	newBatch := func() (*prefixBatch, []string) {
		return &prefixBatch{entries: make([]prefixEntry, 0, prefixBatchSize)}, make([]string, 0, prefixBatchSize)
	}
	batch, prefixes := newBatch()
	ok := true
	err := db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, sk string, _ []byte) bool {
		prefix := stats.DecodeSummaryKey(sep, sk)
		batch.entries = append(batch.entries, prefixEntry{summaryKey: sk, prefix: prefix})
		prefixes = append(prefixes, prefix)
		if len(prefixes) < prefixBatchSize {
			return true
		}
		ok = send(batch, prefixes)
		batch, prefixes = newBatch()
		return ok
	})
	if err != nil {
		return err
	}
	if ok && len(prefixes) > 0 && !send(batch, prefixes) {
		ok = false
	}
	if !ok {
		return ctx.Err()
	}
	return nil
}
