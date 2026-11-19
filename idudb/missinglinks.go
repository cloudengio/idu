// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
)

// Missing links are detected by comparing the link count of every file
// that may be a hardlink, in each prefix whose summary is written by an
// analysis, with the number of links to it recorded in the link index once
// that has been updated. A file with more links than are recorded has links
// that are outside of the analyzed filesystem, are excluded, or are in
// prefixes whose summaries do not record the file because its link count,
// as stored for that prefix, is stale, ie. that prefix has not been
// rescanned since the link was created. Only the last of these leads to
// the file being counted more than once, and listing the prefix that
// contains the other links in hardlinks_record_all prevents it. Analyze
// cannot tell which of these applies, or where the other links are, and
// hence records the prefixes that contain such files as candidates.
const metaKeyMissingLinks = "missinglinks/v1"

// maxMissingLinkExamples is the maximum number of example filenames
// recorded for each prefix with missing links.
const maxMissingLinkExamples = 3

// MissingLinksPrefix describes the files in a prefix that have more links
// than are recorded in the link index.
type MissingLinksPrefix struct {
	Prefix string `json:"prefix"`
	// Files is the number of such files.
	Files int `json:"files"`
	// Links is the total number of links to those files that are not
	// recorded.
	Links uint64 `json:"links"`
	// Examples contains the names of some of those files.
	Examples []string `json:"examples,omitempty"`
	// Updated is the time of the analysis that found them.
	Updated time.Time `json:"updated"`
}

// MissingLinks describes all of the prefixes known to contain files with
// more links than are recorded in the link index, as found by the analyses
// that last wrote the summaries of those prefixes.
type MissingLinks struct {
	Prefixes []MissingLinksPrefix `json:"prefixes"`
}

// ReadMissingLinks returns the prefixes found to contain files with more
// links than are recorded in the link index. They are only found when the
// summaries record the files that may be hardlinks and the top-N rankings
// are enabled, since the link index is maintained along with them.
func ReadMissingLinks(ctx context.Context, db Reader) (MissingLinks, error) {
	var ml MissingLinks
	buf, err := getMeta(ctx, db, metaKeyMissingLinks)
	if err != nil || len(buf) == 0 {
		return ml, err
	}
	if err := json.Unmarshal(buf, &ml); err != nil {
		return MissingLinks{}, fmt.Errorf("invalid missing links: %w", err)
	}
	return ml, nil
}

// seenLinks records the link count of a file and a prefix that contains it.
type seenLinks struct {
	links  uint64
	prefix string
}

// linkCounts records the link counts of the files that may be hardlinks
// in the prefixes whose summaries are written by an analysis. It is safe
// for concurrent use.
type linkCounts struct {
	mu      sync.Mutex
	files   map[inode]seenLinks
	written map[string]bool
	removed []string
}

func newLinkCounts() *linkCounts {
	return &linkCounts{files: map[inode]seenLinks{}, written: map[string]bool{}}
}

// record records the files in pi, the metadata for prefix, that may be
// hardlinks.
func (lc *linkCounts) record(prefix string, pi *prefixinfo.T) {
	dev := pi.XAttr().Device
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.written[prefix] = true
	for _, fi := range pi.InfoList() {
		if fi.IsDir() {
			continue
		}
		xattr := pi.XAttrInfo(fi)
		if xattr.FileID == 0 || xattr.Hardlinks <= 1 {
			continue
		}
		in := inode{dev: dev, ino: xattr.FileID}
		if xattr.Hardlinks > lc.files[in].links {
			lc.files[in] = seenLinks{links: xattr.Hardlinks, prefix: prefix}
		}
	}
}

// deleted records that prefix and all of the prefixes beneath it have been
// deleted.
func (lc *linkCounts) deleted(prefix string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.removed = append(lc.removed, prefix)
}

// fullScanThreshold is the number of files beyond which the entire link
// index is read rather than the entries for each file. It is a variable
// for testing.
var fullScanThreshold = 10000

// recordedLinks returns the number of links recorded in the link index for
// each of the files in lc.
func (lc *linkCounts) recordedLinks(ctx context.Context, db Reader) (map[inode]uint64, error) {
	recorded := make(map[inode]uint64, len(lc.files))
	if len(lc.files) <= fullScanThreshold {
		for in := range lc.files {
			occ, err := readOccurrences(ctx, db, in)
			if err != nil {
				return nil, err
			}
			for _, o := range occ {
				recorded[in] += o.count
			}
		}
		return recorded, nil
	}
	var parseErr error
	err := db.ScanLinks(ctx, string(fileLinkTag), func(_ context.Context, k string, v []byte) bool {
		var in inode
		if in, _, parseErr = parseLinkKey(k); parseErr != nil {
			return false
		}
		if _, ok := lc.files[in]; !ok {
			return true
		}
		c, err := decodeLinkCount(k, v)
		if err != nil {
			parseErr = err
			return false
		}
		recorded[in] += c
		return true
	})
	if err == nil {
		err = parseErr
	}
	return recorded, err
}

// isRemoved returns true if prefix was deleted, or is beneath a prefix
// that was deleted, by the analysis.
func (lc *linkCounts) isRemoved(sep, prefix string) bool {
	for _, r := range lc.removed {
		if prefix == r || strings.HasPrefix(prefix, strings.TrimRight(r, sep)+sep) {
			return true
		}
	}
	return false
}

// updateMissingLinks updates the stored description of the prefixes with
// missing links for the prefixes whose summaries were written by the
// analysis. It must only be called once the link index is up to date.
func (sdb *DB) updateMissingLinks(ctx context.Context, when time.Time) error {
	lc := sdb.links
	if lc == nil {
		return nil
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	prev, err := ReadMissingLinks(ctx, sdb.db)
	if err != nil {
		// Start afresh since the previous state cannot be read.
		prev = MissingLinks{}
	}
	found := map[string]*MissingLinksPrefix{}
	for _, p := range prev.Prefixes {
		if !lc.written[p.Prefix] && !lc.isRemoved(sdb.sep, p.Prefix) {
			found[p.Prefix] = &p
		}
	}
	recorded, err := lc.recordedLinks(ctx, sdb.db)
	if err != nil {
		return err
	}
	examples := map[string][]uint64{}
	for in, seen := range lc.files {
		if seen.links <= recorded[in] {
			continue
		}
		p := found[seen.prefix]
		if p == nil {
			p = &MissingLinksPrefix{Prefix: seen.prefix, Updated: when}
			found[seen.prefix] = p
		}
		p.Files++
		p.Links += seen.links - recorded[in]
		examples[seen.prefix] = append(examples[seen.prefix], in.ino)
	}
	for prefix, inodes := range examples {
		names, err := sdb.fileNames(ctx, prefix, inodes)
		if err != nil {
			return err
		}
		found[prefix].Examples = names
	}
	ml := MissingLinks{Prefixes: make([]MissingLinksPrefix, 0, len(found))}
	for _, prefix := range slices.Sorted(maps.Keys(found)) {
		ml.Prefixes = append(ml.Prefixes, *found[prefix])
	}
	buf, err := json.Marshal(ml)
	if err != nil {
		return err
	}
	return sdb.db.SetMeta(ctx, metaKeyMissingLinks, buf)
}

// fileNames returns the names, in order, of up to maxMissingLinkExamples
// of the files in prefix with the supplied inode numbers.
func (sdb *DB) fileNames(ctx context.Context, prefix string, inodes []uint64) ([]string, error) {
	var pi prefixinfo.T
	ok, err := sdb.getPrefixInfo(ctx, prefix, &pi)
	if err != nil || !ok {
		return nil, err
	}
	want := make(map[uint64]bool, len(inodes))
	for _, ino := range inodes {
		want[ino] = true
	}
	var names []string
	for _, fi := range pi.InfoList() {
		if !fi.IsDir() && want[pi.XAttrInfo(fi).FileID] {
			names = append(names, fi.Name())
		}
	}
	slices.Sort(names)
	return names[:min(len(names), maxMissingLinkExamples)], nil
}

// recordsLinks returns true if the analysis should record the link counts
// of the files in the summaries that it writes, ie. if the link index is
// maintained.
func (sdb *DB) recordsLinks() bool {
	return sdb.topn.enabled() && sdb.hardlinkInfo != stats.RecordNoFiles
}
