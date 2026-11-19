// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/binary"
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/file/diskusage"
)

func trimTrailingSep(sep, path string) string {
	if len(path) <= len(sep) {
		return path
	}
	// Preserve Windows drive roots such as "C:\".
	if len(path) == 2+len(sep) && path[1] == ':' && strings.HasSuffix(path, sep) {
		return path
	}
	return strings.TrimRight(path, sep)
}

// NormalizePath trims any trailing path separator from path using sep
// (defaulting to "/"), while preserving filesystem root paths such as "/"
// or "C:\".
func NormalizePath(sep, path string) string {
	if sep == "" {
		sep = "/"
	}
	return trimTrailingSep(sep, path)
}

// SummaryKeys converts a filesystem path and separator into the exact key and
// child-prefix key used by the ordered summary bucket. Replacing the path
// separator with NUL ('\x00') ensures lexicographical ordering matches DFS
// pre-order and prevents sibling names like "/a-b" from sorting inside "/a/...".
func SummaryKeys(sep, path string) (exactKey, childPrefix string) {
	if sep == "" {
		sep = "/"
	}
	if path == "" {
		return "", ""
	}
	norm := NormalizePath(sep, path)
	exactKey = strings.ReplaceAll(norm, sep, "\x00")
	if strings.HasSuffix(norm, sep) {
		childPrefix = exactKey
	} else {
		childPrefix = exactKey + "\x00"
	}
	return exactKey, childPrefix
}

// DecodeSummaryKey reverses SummaryKeys back to a filesystem path.
func DecodeSummaryKey(sep, key string) string {
	if sep == "" {
		sep = "/"
	}
	return strings.ReplaceAll(key, "\x00", sep)
}

// ParentSummaryKey returns the summary key of the parent of the prefix
// with summary key key, or false if key is that of a filesystem root.
func ParentSummaryKey(key string) (string, bool) {
	idx := strings.LastIndexByte(key, 0)
	if idx < 0 {
		return "", false
	}
	parent := key[:idx]
	if strings.IndexByte(parent, 0) < 0 {
		// The parent is a root, eg. "/" or "C:\", whose key retains its
		// trailing separator.
		parent = key[:idx+1]
	}
	if parent == key {
		return "", false
	}
	return parent, true
}

// IsSummaryDescendant returns true if childKey is the summary key of a
// descendant of the prefix with summary key parentKey.
func IsSummaryDescendant(parentKey, childKey string) bool {
	if parentKey == "" {
		return false
	}
	// Only a root key (e.g. "/" encoded as "\x00") ends with the separator,
	// in which case every longer key with that prefix is a descendant.
	if strings.HasSuffix(parentKey, "\x00") {
		return len(childKey) > len(parentKey) && strings.HasPrefix(childKey, parentKey)
	}
	return len(childKey) > len(parentKey)+1 &&
		strings.HasPrefix(childKey, parentKey) &&
		childKey[len(parentKey)] == '\x00'
}

// PrefixSummary stores precomputed direct (non-recursive) statistics for a
// single prefix along with the calculator identifier used for StorageBytes.
type PrefixSummary struct {
	CalcID   string
	Direct   Totals
	PerUser  PerIDTotals
	PerGroup PerIDTotals

	// HardlinkInfo is true if the summary records the information needed
	// to deduplicate hardlinks, i.e. Device, FileID and Files, via
	// ApplyHardlinks.
	HardlinkInfo bool
	// Device and FileID identify the prefix itself.
	Device, FileID uint64
	// Files lists every file counted in Direct. All files are recorded,
	// rather than only those with a link count greater than one, since
	// a hardlink may be created to a file in a prefix that is not rescanned
	// because it is unchanged.
	Files []FileEntry
}

// FileEntry records the contribution of a file to the totals of the prefix
// that contains it.
type FileEntry struct {
	FileID       uint64
	UID, GID     int64
	Bytes        int64
	StorageBytes int64
	SparseBytes  int64
}

// ComputeSummary computes a PrefixSummary for a single directory/prefix.
// If hardlinkInfo is true the summary records the information required
// for hardlinks to be deduplicated by ApplyHardlinks. match should not
// perform hardlink handling itself.
func ComputeSummary(prefix string, pi PrefixInfo, du diskusage.Calculator, match Matcher, hardlinkInfo bool) PrefixSummary {
	direct, users, groups := ComputeTotals(prefix, pi, du, match)
	calcID := ""
	if du != nil {
		calcID = du.String()
	}
	ps := PrefixSummary{
		CalcID:   calcID,
		Direct:   direct,
		PerUser:  users,
		PerGroup: groups,
	}
	if hardlinkInfo {
		ps.HardlinkInfo = true
		xattr := pi.XAttr()
		ps.Device, ps.FileID = xattr.Device, xattr.FileID
		ps.Files = computeFileEntries(prefix, pi, du, match)
	}
	return ps
}

// computeFileEntries returns the files counted by ComputeTotals.
func computeFileEntries(prefix string, pi PrefixInfo, du diskusage.Calculator, match Matcher) []FileEntry {
	if !match.Prefix(prefix, pi) {
		return nil
	}
	var files []FileEntry
	for _, fi := range pi.InfoList() {
		if fi.IsDir() || !match.Entry(prefix, pi, fi) {
			continue
		}
		xattr := pi.XAttrInfo(fi)
		files = append(files, FileEntry{
			FileID:       xattr.FileID,
			UID:          xattr.UID,
			GID:          xattr.GID,
			Bytes:        fi.Size(),
			StorageBytes: du.Calculate(fi.Size(), xattr.Blocks),
			SparseBytes:  SparseBytes(fi.Size(), xattr.Blocks),
		})
	}
	return files
}

// ApplyHardlinks adjusts ps to exclude the prefix itself, or any of its
// files, that have already been seen by hl, recording them as hardlinks
// instead. Applying it to summaries in the same order as ComputeTotals is
// called with a hardlink-handling matcher yields identical results.
// It has no effect if HardlinkInfo is false.
func (ps *PrefixSummary) ApplyHardlinks(hl *hardlinks.Incremental) {
	if !ps.HardlinkInfo {
		return
	}
	if hl.Ref(ps.Device, ps.FileID) {
		ps.Direct = Totals{Prefix: 1, HardlinkDirs: 1}
		ps.PerUser, ps.PerGroup, ps.Files = nil, nil, nil
		return
	}
	for _, l := range ps.Files {
		// Files share the device of their prefix.
		if !hl.Ref(ps.Device, l.FileID) {
			continue
		}
		ps.Direct = ps.Direct.asHardlink(l)
		ps.PerUser.asHardlink(l.UID, l)
		ps.PerGroup.asHardlink(l.GID, l)
	}
}

func (t Totals) asHardlink(l FileEntry) Totals {
	t.Files--
	t.Bytes -= l.Bytes
	t.StorageBytes -= l.StorageBytes
	if l.SparseBytes > 0 {
		t.SparseFiles--
		t.SparseBytes -= l.SparseBytes
	}
	t.Hardlinks++
	return t
}

func (pid PerIDTotals) asHardlink(id int64, l FileEntry) {
	for i := range pid {
		if pid[i].ID == id {
			pid[i] = pid[i].asHardlink(l)
			return
		}
	}
}

// PrefixSummaryVersion is the version of the binary encoding of
// PrefixSummary, which includes that of Totals.
const PrefixSummaryVersion = 0x3

// AppendBinary appends the binary encoding of ps to data and returns the
// extended slice.
func (ps *PrefixSummary) AppendBinary(data []byte) []byte {
	data = binary.AppendVarint(data, PrefixSummaryVersion)
	data = binary.AppendUvarint(data, uint64(len(ps.CalcID)))
	data = append(data, ps.CalcID...)
	data = ps.Direct.AppendBinary(data)
	data = ps.PerUser.AppendBinary(data)
	data = ps.PerGroup.AppendBinary(data)
	if !ps.HardlinkInfo {
		return binary.AppendUvarint(data, 0)
	}
	data = binary.AppendUvarint(data, 1)
	data = binary.AppendUvarint(data, ps.Device)
	data = binary.AppendUvarint(data, ps.FileID)
	data = binary.AppendUvarint(data, uint64(len(ps.Files)))
	for _, l := range ps.Files {
		data = binary.AppendUvarint(data, l.FileID)
		data = binary.AppendVarint(data, l.UID)
		data = binary.AppendVarint(data, l.GID)
		data = binary.AppendVarint(data, l.Bytes)
		data = binary.AppendVarint(data, l.StorageBytes)
		data = binary.AppendVarint(data, l.SparseBytes)
	}
	return data
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (ps *PrefixSummary) MarshalBinary() ([]byte, error) {
	return ps.AppendBinary(make([]byte, 0, 128)), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (ps *PrefixSummary) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("PrefixSummary: empty buffer")
	}
	ver, n := binary.Varint(data)
	if n <= 0 || ver != PrefixSummaryVersion {
		return fmt.Errorf("PrefixSummary: unsupported version %d", ver)
	}
	data = data[n:]

	calcLen, n := binary.Uvarint(data)
	if n <= 0 || uint64(len(data)-n) < calcLen {
		return fmt.Errorf("PrefixSummary: truncated calc id")
	}
	data = data[n:]
	ps.CalcID = string(data[:calcLen])
	data = data[calcLen:]

	var err error
	data, err = decodeTotalsChecked(&ps.Direct, data)
	if err != nil {
		return fmt.Errorf("PrefixSummary: direct totals: %w", err)
	}
	data, err = decodePerIDChecked(&ps.PerUser, data)
	if err != nil {
		return fmt.Errorf("PrefixSummary: per-user totals: %w", err)
	}
	data, err = decodePerIDChecked(&ps.PerGroup, data)
	if err != nil {
		return fmt.Errorf("PrefixSummary: per-group totals: %w", err)
	}
	if err := ps.decodeFiles(data); err != nil {
		return fmt.Errorf("PrefixSummary: files: %w", err)
	}
	return nil
}

func readUvarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("truncated uvarint")
	}
	return v, data[n:], nil
}

func (ps *PrefixSummary) decodeFiles(data []byte) error {
	ps.HardlinkInfo, ps.Device, ps.FileID, ps.Files = false, 0, 0, nil
	recorded, data, err := readUvarint(data)
	if err != nil || recorded == 0 {
		return err
	}
	ps.HardlinkInfo = true
	if ps.Device, data, err = readUvarint(data); err != nil {
		return err
	}
	if ps.FileID, data, err = readUvarint(data); err != nil {
		return err
	}
	count, data, err := readUvarint(data)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	if count > uint64(len(data)) {
		return fmt.Errorf("invalid length %d", count)
	}
	ps.Files = make([]FileEntry, count)
	for i := range ps.Files {
		l := &ps.Files[i]
		if l.FileID, data, err = readUvarint(data); err != nil {
			return err
		}
		for _, f := range []*int64{&l.UID, &l.GID, &l.Bytes, &l.StorageBytes, &l.SparseBytes} {
			if *f, data, err = readVarint(data); err != nil {
				return err
			}
		}
	}
	return nil
}

func readVarint(data []byte) (int64, []byte, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("truncated varint")
	}
	return v, data[n:], nil
}

func decodeTotalsChecked(t *Totals, data []byte) ([]byte, error) {
	ver, rest, err := readVarint(data)
	if err != nil {
		return nil, err
	}
	if ver != totalsVersion {
		return nil, fmt.Errorf("unsupported totals version %d", ver)
	}
	fields := []*int64{
		&t.ID, &t.Files, &t.Bytes, &t.StorageBytes, &t.PrefixBytes,
		&t.Prefix, &t.SubPrefixes, &t.Hardlinks, &t.HardlinkDirs,
		&t.SparseFiles, &t.SparseBytes,
	}
	for _, f := range fields {
		var val int64
		val, rest, err = readVarint(rest)
		if err != nil {
			return nil, err
		}
		*f = val
	}
	return rest, nil
}

func decodePerIDChecked(pid *PerIDTotals, data []byte) ([]byte, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("truncated per-id length")
	}
	rest := data[n:]
	if count == 0 {
		*pid = nil
		return rest, nil
	}
	if count > uint64(len(rest)) {
		return nil, fmt.Errorf("invalid per-id length %d", count)
	}
	out := make(PerIDTotals, count)
	var err error
	for i := range out {
		rest, err = decodeTotalsChecked(&out[i], rest)
		if err != nil {
			return nil, err
		}
	}
	*pid = out
	return rest, nil
}
