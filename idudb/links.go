// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"strings"

	"cloudeng.io/cmd/idu/stats"
)

// The link index allows the state derived from the summaries, ie. the
// recursive totals, rankings and per-id totals, to deduplicate hardlinks
// incrementally. As for summary.Walk, a file is attributed to the first
// prefix, in summary key order, that records it and any other prefixes
// that record it count it as a hardlink. A prefix that is a duplicate of
// a preceding one, eg. a bind mount or a directory hardlink, is counted as
// a directory hardlink and its files are ignored.
//
// The index contains an entry for every prefix that records a file, keyed
// by the file's device and inode numbers followed by the prefix's summary
// key, so that the first entry for a file is its owner. Its value is the
// number of times the prefix records the file. It also contains an entry
// for every prefix that shares its device and inode numbers with another
// prefix. Detecting such prefixes requires knowing the device and inode
// numbers of every prefix and hence they are only found when the index is
// rebuilt; subsequent analyses maintain the entries for the device and
// inode numbers already found.
//
// The index is part of the state derived from the summaries and is valid
// whenever the recursive totals are, see CheckSubtrees.
const (
	fileLinkTag byte = 'f'
	dirLinkTag  byte = 'd'
)

// inode identifies a file or prefix.
type inode struct {
	dev, ino uint64
}

// linkPrefix returns the prefix shared by the link index keys of in.
func linkPrefix(tag byte, in inode) string {
	var b [17]byte
	b[0] = tag
	binary.BigEndian.PutUint64(b[1:], in.dev)
	binary.BigEndian.PutUint64(b[9:], in.ino)
	return string(b[:])
}

func linkKey(tag byte, in inode, key string) string {
	return linkPrefix(tag, in) + key
}

// parseLinkKey returns the inode and summary key of a link index key.
func parseLinkKey(k string) (inode, string, error) {
	if len(k) < 17 {
		return inode{}, "", fmt.Errorf("link index: %w: invalid key %q", errInconsistentSubtrees, k)
	}
	return inode{
		dev: binary.BigEndian.Uint64([]byte(k[1:9])),
		ino: binary.BigEndian.Uint64([]byte(k[9:17])),
	}, k[17:], nil
}

// occurrence records that the prefix with summary key key records a file,
// count times.
type occurrence struct {
	key   string
	count uint64
}

// readOccurrences returns the occurrences of the file in, in key order.
func readOccurrences(ctx context.Context, db Reader, in inode) ([]occurrence, error) {
	prefix := linkPrefix(fileLinkTag, in)
	var occ []occurrence
	var decodeErr error
	err := db.ScanLinks(ctx, prefix, func(_ context.Context, k string, v []byte) bool {
		c, n := binary.Uvarint(v)
		if n <= 0 || n != len(v) || c == 0 {
			decodeErr = fmt.Errorf("link index: %w: invalid count for %q", errInconsistentSubtrees, k)
			return false
		}
		occ = append(occ, occurrence{key: k[len(prefix):], count: c})
		return true
	})
	if err == nil {
		err = decodeErr
	}
	return occ, err
}

func firstKey(occ []occurrence) string {
	if len(occ) == 0 {
		return ""
	}
	return occ[0].key
}

// dirIndex holds the prefixes that share their device and inode numbers
// with another prefix, or did when the index was rebuilt.
type dirIndex struct {
	inodes map[inode][]string // sorted summary keys
	keys   map[string]inode
}

func newDirIndex() *dirIndex {
	return &dirIndex{inodes: map[inode][]string{}, keys: map[string]inode{}}
}

func loadDirIndex(ctx context.Context, db Reader) (*dirIndex, error) {
	d := newDirIndex()
	var parseErr error
	err := db.ScanLinks(ctx, string(dirLinkTag), func(_ context.Context, k string, _ []byte) bool {
		var in inode
		var key string
		if in, key, parseErr = parseLinkKey(k); parseErr != nil {
			return false
		}
		d.add(in, key)
		return true
	})
	if err == nil {
		err = parseErr
	}
	return d, err
}

func (d *dirIndex) clone() *dirIndex {
	c := newDirIndex()
	for in, keys := range d.inodes {
		c.inodes[in] = slices.Clone(keys)
	}
	maps.Copy(c.keys, d.keys)
	return c
}

func (d *dirIndex) add(in inode, key string) {
	keys := d.inodes[in]
	i, found := slices.BinarySearch(keys, key)
	if !found {
		d.inodes[in] = slices.Insert(keys, i, key)
	}
	d.keys[key] = in
}

func (d *dirIndex) remove(key string) (inode, bool) {
	in, ok := d.keys[key]
	if !ok {
		return in, false
	}
	delete(d.keys, key)
	keys := slices.DeleteFunc(d.inodes[in], func(k string) bool { return k == key })
	if len(keys) == 0 {
		delete(d.inodes, in)
	} else {
		d.inodes[in] = keys
	}
	return in, true
}

// isDup returns true if the prefix with summary key key is a duplicate of
// a preceding prefix.
func (d *dirIndex) isDup(key string) bool {
	in, ok := d.keys[key]
	return ok && d.inodes[in][0] != key
}

func (d *dirIndex) tracked(in inode) bool {
	_, ok := d.inodes[in]
	return ok
}

func (d *dirIndex) contains(key string) bool {
	_, ok := d.keys[key]
	return ok
}

// dirInode returns the device and inode numbers of the prefix described
// by ps, if they are recorded.
func dirInode(ps *stats.PrefixSummary) (inode, bool) {
	if ps == nil || !ps.HardlinkInfo || ps.FileID == 0 {
		return inode{}, false
	}
	return inode{dev: ps.Device, ino: ps.FileID}, true
}

// fileCounts returns the number of times that ps records each file.
func fileCounts(ps *stats.PrefixSummary) map[inode]uint64 {
	if ps == nil || !ps.HardlinkInfo || len(ps.Files) == 0 {
		return nil
	}
	counts := make(map[inode]uint64, len(ps.Files))
	for _, f := range ps.Files {
		if f.FileID != 0 {
			counts[inode{dev: ps.Device, ino: f.FileID}]++
		}
	}
	return counts
}

func cloneSummary(ps *stats.PrefixSummary) *stats.PrefixSummary {
	if ps == nil {
		return nil
	}
	c := *ps
	c.PerUser = slices.Clone(ps.PerUser)
	c.PerGroup = slices.Clone(ps.PerGroup)
	c.Files = slices.Clone(ps.Files)
	return &c
}

// linkScanner deduplicates hardlinks for a pass over all of the summaries
// in key order. If rebuild is true it detects duplicate prefixes and
// writes the link index, which must be empty, as it does so, otherwise it
// uses the existing index to identify duplicate prefixes.
type linkScanner struct {
	db        Database
	rebuild   bool
	dirs      *dirIndex
	seenDirs  map[inode]bool
	dupDirs   map[inode][]string
	seenFiles map[inode]bool
}

func newLinkScanner(ctx context.Context, db Database, rebuild bool) (*linkScanner, error) {
	ls := &linkScanner{
		db:        db,
		rebuild:   rebuild,
		seenFiles: map[inode]bool{},
	}
	if rebuild {
		ls.seenDirs = map[inode]bool{}
		ls.dupDirs = map[inode][]string{}
		return ls, nil
	}
	var err error
	ls.dirs, err = loadDirIndex(ctx, db)
	return ls, err
}

// effective returns ps, the summary for key, with its hardlinks
// deduplicated.
func (ls *linkScanner) effective(ctx context.Context, key string, ps *stats.PrefixSummary) (stats.PrefixSummary, error) {
	if !ps.HardlinkInfo {
		return *ps, nil
	}
	dup := false
	if in, ok := dirInode(ps); ok {
		switch {
		case !ls.rebuild:
			dup = ls.dirs.isDup(key)
		case ls.seenDirs[in]:
			dup = true
			ls.dupDirs[in] = append(ls.dupDirs[in], key)
		default:
			ls.seenDirs[in] = true
		}
	}
	eff := ps.Effective(dup, func(ino uint64) bool {
		in := inode{dev: ps.Device, ino: ino}
		if ls.seenFiles[in] {
			return false
		}
		ls.seenFiles[in] = true
		return true
	})
	if !ls.rebuild || dup {
		return eff, nil
	}
	for in, count := range fileCounts(ps) {
		if err := ls.db.SetLink(ctx, linkKey(fileLinkTag, in, key), binary.AppendUvarint(nil, count), true); err != nil {
			return eff, err
		}
	}
	return eff, nil
}

// finish writes the entries for the duplicate prefixes found by a rebuild,
// which requires a second pass over the summaries to find the first
// prefix with each of their device and inode numbers.
func (ls *linkScanner) finish(ctx context.Context) error {
	if !ls.rebuild || len(ls.dupDirs) == 0 {
		return nil
	}
	first := map[inode]string{}
	var ps stats.PrefixSummary
	var decodeErr error
	err := ls.db.ScanSummaries(ctx, "", "", func(_ context.Context, key string, val []byte) bool {
		if decodeErr = ps.UnmarshalBinary(val); decodeErr != nil {
			return false
		}
		if in, ok := dirInode(&ps); ok && ls.dupDirs[in] != nil {
			if _, found := first[in]; !found {
				first[in] = key
			}
		}
		return len(first) < len(ls.dupDirs)
	})
	if err == nil {
		err = decodeErr
	}
	if err != nil {
		return err
	}
	for in, keys := range ls.dupDirs {
		if key, ok := first[in]; ok {
			keys = append(keys, key)
		}
		for _, key := range keys {
			if err := ls.db.SetLink(ctx, linkKey(dirLinkTag, in, key), nil, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// changedPrefix is a prefix whose summary changed, from old to cur, and
// whose effective totals depend on the link index.
type changedPrefix struct {
	path     string
	old, cur *stats.PrefixSummary
}

// linkChanges accumulates the changes, made by an analysis, to the
// prefixes whose effective totals depend on the link index, so that the
// index can be updated and the changes to their effective totals, and
// those of any other prefixes that they affect, computed once the analysis
// has completed. It is not safe for concurrent use.
type linkChanges struct {
	dirs    *dirIndex
	changed map[string]*changedPrefix
	// deleted contains the summary keys of the deleted prefixes that
	// appear in the link index.
	deleted      map[string]bool
	removedFiles map[inode]bool
	// owners caches the owners of files according to the link index as
	// it was before the analysis.
	owners map[inode]string
}

func newLinkChanges(dirs *dirIndex) *linkChanges {
	return &linkChanges{
		dirs:         dirs,
		changed:      map[string]*changedPrefix{},
		deleted:      map[string]bool{},
		removedFiles: map[inode]bool{},
		owners:       map[inode]string{},
	}
}

// relevant returns true if the effective totals of the prefix with
// summary key key, whose summary changed from old to cur, depend on the
// link index, ie. if they may differ from its summary's totals.
func (lc *linkChanges) relevant(key string, old, cur *stats.PrefixSummary) bool {
	if lc.dirs.contains(key) {
		return true
	}
	for _, ps := range []*stats.PrefixSummary{old, cur} {
		if ps == nil || !ps.HardlinkInfo {
			continue
		}
		if len(ps.Files) > 0 {
			return true
		}
		if in, ok := dirInode(ps); ok && lc.dirs.tracked(in) {
			return true
		}
	}
	return false
}

// prefixChanged records a change to a prefix for which relevant is true.
func (lc *linkChanges) prefixChanged(key, path string, old, cur *stats.PrefixSummary) {
	lc.changed[key] = &changedPrefix{path: path, old: cloneSummary(old), cur: cloneSummary(cur)}
}

// owner returns the owner of the file in according to the link index as
// it was before the analysis.
func (lc *linkChanges) owner(ctx context.Context, db Reader, in inode) (string, error) {
	if o, ok := lc.owners[in]; ok {
		return o, nil
	}
	occ, err := readOccurrences(ctx, db, in)
	if err != nil {
		return "", err
	}
	o := firstKey(occ)
	lc.owners[in] = o
	return o, nil
}

// effectiveFn returns the effective summary of ps, the summary for key,
// given whether it is a duplicate and the owner of each of its files.
func effectiveFn(key string, ps *stats.PrefixSummary, dup bool, owner func(inode) (string, error)) (stats.PrefixSummary, error) {
	var ownErr error
	eff := ps.Effective(dup, func(ino uint64) bool {
		o, err := owner(inode{dev: ps.Device, ino: ino})
		if err != nil && ownErr == nil {
			ownErr = err
		}
		return o == key
	})
	return eff, ownErr
}

// subtreeDeleted records that the prefix with summary key key, whose
// summary is ps, is being deleted and returns its effective summary.
func (lc *linkChanges) subtreeDeleted(ctx context.Context, db Reader, key string, ps *stats.PrefixSummary) (stats.PrefixSummary, error) {
	if !ps.HardlinkInfo {
		return *ps, nil
	}
	dup := lc.dirs.isDup(key)
	if lc.dirs.contains(key) {
		lc.deleted[key] = true
	}
	if !dup {
		for in := range fileCounts(ps) {
			lc.removedFiles[in] = true
			lc.deleted[key] = true
		}
	}
	return effectiveFn(key, ps, dup, func(in inode) (string, error) {
		return lc.owner(ctx, db, in)
	})
}

// effectiveChange is a change to the effective summary of a prefix.
type effectiveChange struct {
	key, path string
	old, cur  *stats.PrefixSummary // old is nil for a new prefix
}

// linkUpdate computes the changes to the link index, and to the
// effective summaries, that result from a linkChanges.
type linkUpdate struct {
	lc               *linkChanges
	db               Database
	sep              string
	isDeleted        func(key string) bool
	oldDirs, newDirs *dirIndex
	// unchanged holds the summaries of the prefixes that were not changed
	// by the analysis but whose effective summaries may have, either
	// because they are no longer, or have become, duplicates or because
	// they have gained or lost the ownership of a file.
	unchanged map[string]*stats.PrefixSummary
	oldOcc    map[string]map[inode]uint64
	newOcc    map[string]map[inode]uint64
	oldOwners map[inode]string
	newOwners map[inode]string
}

// apply updates the link index and returns the resulting changes to the
// effective summaries, in key order. isDeleted must return true for the
// summary keys of all of the prefixes deleted by the analysis.
func (lc *linkChanges) apply(ctx context.Context, db Database, sep string, isDeleted func(key string) bool) ([]effectiveChange, error) {
	for key := range lc.changed {
		if isDeleted(key) {
			delete(lc.changed, key)
		}
	}
	lu := &linkUpdate{
		lc:        lc,
		db:        db,
		sep:       sep,
		isDeleted: isDeleted,
		oldDirs:   lc.dirs,
		unchanged: map[string]*stats.PrefixSummary{},
		oldOcc:    map[string]map[inode]uint64{},
		newOcc:    map[string]map[inode]uint64{},
		oldOwners: map[inode]string{},
		newOwners: map[inode]string{},
	}
	if err := lu.updateDirs(ctx); err != nil {
		return nil, err
	}
	lu.collectOccurrences()
	if err := lu.updateFiles(ctx); err != nil {
		return nil, err
	}
	return lu.changes(ctx)
}

// isKnown returns true if key's summary was changed, or deleted, by the
// analysis, or has already been read.
func (lu *linkUpdate) isKnown(key string) bool {
	_, unchanged := lu.unchanged[key]
	return lu.lc.changed[key] != nil || unchanged || lu.isDeleted(key)
}

// readUnchanged reads the summaries of keys, which must not have been
// changed by the analysis.
func (lu *linkUpdate) readUnchanged(ctx context.Context, keys map[string]bool) error {
	for key := range keys {
		ps, err := lu.dbSummary(ctx, key)
		if err != nil {
			return err
		}
		if ps == nil {
			return fmt.Errorf("%v: %w: missing summary", stats.DecodeSummaryKey(lu.sep, key), errInconsistentSubtrees)
		}
		lu.unchanged[key] = ps
	}
	return nil
}

func (lu *linkUpdate) dbSummary(ctx context.Context, key string) (*stats.PrefixSummary, error) {
	var ps *stats.PrefixSummary
	var decodeErr error
	err := lu.db.ScanSummaries(ctx, key, "", func(_ context.Context, _ string, val []byte) bool {
		ps = &stats.PrefixSummary{}
		decodeErr = ps.UnmarshalBinary(val)
		return false
	})
	if err == nil {
		err = decodeErr
	}
	return ps, err
}

// updateDirs computes and writes the changes to the duplicate prefixes and
// reads the summaries of the unchanged prefixes that are no longer, or
// have become, duplicates.
func (lu *linkUpdate) updateDirs(ctx context.Context) error {
	lu.newDirs = lu.oldDirs.clone()
	touched := map[inode]bool{}
	for key, cp := range lu.lc.changed {
		if in, ok := lu.newDirs.remove(key); ok {
			touched[in] = true
		}
		if in, ok := dirInode(cp.cur); ok && lu.oldDirs.tracked(in) {
			lu.newDirs.add(in, key)
			touched[in] = true
		}
	}
	for key := range lu.lc.deleted {
		if in, ok := lu.newDirs.remove(key); ok {
			touched[in] = true
		}
	}
	flipped := map[string]bool{}
	for in := range touched {
		oldKeys, newKeys := lu.oldDirs.inodes[in], lu.newDirs.inodes[in]
		if err := lu.writeDirs(ctx, in, oldKeys, newKeys); err != nil {
			return err
		}
		for _, key := range slices.Concat(oldKeys, newKeys) {
			if !lu.isKnown(key) && lu.oldDirs.isDup(key) != lu.newDirs.isDup(key) {
				flipped[key] = true
			}
		}
	}
	return lu.readUnchanged(ctx, flipped)
}

// writeDirs writes the changes to the prefixes with the device and inode
// numbers in.
func (lu *linkUpdate) writeDirs(ctx context.Context, in inode, oldKeys, newKeys []string) error {
	for _, key := range oldKeys {
		if !slices.Contains(newKeys, key) {
			if err := lu.db.DeleteLink(ctx, linkKey(dirLinkTag, in, key)); err != nil {
				return err
			}
		}
	}
	for _, key := range newKeys {
		if !slices.Contains(oldKeys, key) {
			if err := lu.db.SetLink(ctx, linkKey(dirLinkTag, in, key), nil, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// collectOccurrences computes the files recorded by each of the changed
// prefixes, and the unchanged ones that are no longer, or have become,
// duplicates, before and after the analysis.
func (lu *linkUpdate) collectOccurrences() {
	for key, cp := range lu.lc.changed {
		if !lu.oldDirs.isDup(key) {
			lu.oldOcc[key] = fileCounts(cp.old)
		}
		if !lu.newDirs.isDup(key) {
			lu.newOcc[key] = fileCounts(cp.cur)
		}
	}
	for key, ps := range lu.unchanged {
		if !lu.oldDirs.isDup(key) {
			lu.oldOcc[key] = fileCounts(ps)
		}
		if !lu.newDirs.isDup(key) {
			lu.newOcc[key] = fileCounts(ps)
		}
	}
}

// updateFiles computes and writes the changes to the occurrences of every
// file affected by the analysis and reads the summaries of the unchanged
// prefixes that have gained or lost the ownership of a file.
func (lu *linkUpdate) updateFiles(ctx context.Context) error {
	affected := maps.Clone(lu.lc.removedFiles)
	for _, occ := range []map[string]map[inode]uint64{lu.oldOcc, lu.newOcc} {
		for _, counts := range occ {
			for in := range counts {
				affected[in] = true
			}
		}
	}
	owners := map[string]bool{}
	for _, in := range slices.SortedFunc(maps.Keys(affected), compareInodes) {
		oldOcc, err := readOccurrences(ctx, lu.db, in)
		if err != nil {
			return err
		}
		newOcc := lu.newOccurrences(in, oldOcc)
		if err := lu.writeOccurrences(ctx, in, oldOcc, newOcc); err != nil {
			return err
		}
		oldOwner, newOwner := firstKey(oldOcc), firstKey(newOcc)
		lu.oldOwners[in], lu.newOwners[in] = oldOwner, newOwner
		if oldOwner == newOwner {
			continue
		}
		for _, key := range []string{oldOwner, newOwner} {
			if key != "" && !lu.isKnown(key) {
				owners[key] = true
			}
		}
	}
	return lu.readUnchanged(ctx, owners)
}

func compareInodes(a, b inode) int {
	if c := cmp.Compare(a.dev, b.dev); c != 0 {
		return c
	}
	return cmp.Compare(a.ino, b.ino)
}

// newOccurrences returns the occurrences of the file in after the analysis
// given those before it.
func (lu *linkUpdate) newOccurrences(in inode, oldOcc []occurrence) []occurrence {
	var occ []occurrence
	for _, o := range oldOcc {
		// The occurrences of the changed prefixes, and of the unchanged
		// ones that are no longer, or have become, duplicates, are
		// recomputed from their summaries.
		if lu.isKnown(o.key) {
			continue
		}
		occ = append(occ, o)
	}
	for key, counts := range lu.newOcc {
		if c := counts[in]; c > 0 {
			occ = append(occ, occurrence{key: key, count: c})
		}
	}
	slices.SortFunc(occ, func(a, b occurrence) int { return strings.Compare(a.key, b.key) })
	return occ
}

func (lu *linkUpdate) writeOccurrences(ctx context.Context, in inode, oldOcc, newOcc []occurrence) error {
	counts := make(map[string]uint64, len(newOcc))
	for _, o := range newOcc {
		counts[o.key] = o.count
	}
	for _, o := range oldOcc {
		if _, ok := counts[o.key]; ok {
			continue
		}
		if err := lu.db.DeleteLink(ctx, linkKey(fileLinkTag, in, o.key)); err != nil {
			return err
		}
	}
	prev := make(map[string]uint64, len(oldOcc))
	for _, o := range oldOcc {
		prev[o.key] = o.count
	}
	for _, o := range newOcc {
		if prev[o.key] == o.count {
			continue
		}
		if err := lu.db.SetLink(ctx, linkKey(fileLinkTag, in, o.key), binary.AppendUvarint(nil, o.count), true); err != nil {
			return err
		}
	}
	return nil
}

// owner returns the owner of the file in before, or after, the analysis.
func (lu *linkUpdate) owner(ctx context.Context, after bool, in inode) (string, error) {
	owners := lu.oldOwners
	if after {
		owners = lu.newOwners
	}
	if o, ok := owners[in]; ok {
		return o, nil
	}
	// The analysis did not affect the occurrences of in.
	return lu.lc.owner(ctx, lu.db, in)
}

// changes returns the changes to the effective summaries of the changed
// prefixes and of the unchanged ones that are affected by them.
func (lu *linkUpdate) changes(ctx context.Context) ([]effectiveChange, error) {
	effective := func(key string, ps *stats.PrefixSummary, after bool) (*stats.PrefixSummary, error) {
		if ps == nil {
			return nil, nil
		}
		dirs := lu.oldDirs
		if after {
			dirs = lu.newDirs
		}
		eff, err := effectiveFn(key, ps, dirs.isDup(key), func(in inode) (string, error) {
			return lu.owner(ctx, after, in)
		})
		return &eff, err
	}
	var changes []effectiveChange
	add := func(key, path string, old, cur *stats.PrefixSummary) error {
		oldEff, err := effective(key, old, false)
		if err != nil {
			return err
		}
		curEff, err := effective(key, cur, true)
		if err != nil {
			return err
		}
		changes = append(changes, effectiveChange{key: key, path: path, old: oldEff, cur: curEff})
		return nil
	}
	for key, cp := range lu.lc.changed {
		if err := add(key, cp.path, cp.old, cp.cur); err != nil {
			return nil, err
		}
	}
	for key, ps := range lu.unchanged {
		if err := add(key, stats.DecodeSummaryKey(lu.sep, key), ps, ps); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(changes, func(a, b effectiveChange) int { return strings.Compare(a.key, b.key) })
	return changes, nil
}
