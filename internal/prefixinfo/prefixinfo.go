// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"time"

	"cloudeng.io/file"
)

// T represents the information for a prefix, ie. a directory.
// It is primarily intended to be stored in a key/value store or database
// and hence attention is paid to minizing its storage requirements.
// In particular, the name of the prefix is not stored as it is assumed
// to be implicitly known from the store's key or database entry.

type T struct {
	xattr      file.XAttr
	size       int64
	mode       fs.FileMode
	modTime    time.Time
	entries    file.InfoList // files and prefixes only
	inodes     []uint64
	blocks     []int64
	links      []uint64 // link counts, zero if unknown and for prefixes
	userIDMap  idMaps
	groupIDMap idMaps
	finalized  bool
}

// New creates a new PrefixInfo for the supplied file.Info. It assumes that
// the supplied file.Info contains a file.XAttr in its Sys() value.
func New(_ string, info file.Info) T {
	pi := T{
		size:    info.Size(),
		modTime: info.ModTime(),
		mode:    info.Mode(),
	}
	switch v := info.Sys().(type) {
	case file.XAttr:
		pi.xattr = v
	case *file.XAttr:
		pi.xattr = *v
	default:
		panic(fmt.Sprintf("invalid system information: %T", v))
	}
	return pi
}

func (pi *T) resetDerived() {
	pi.finalized = false
	pi.inodes = nil
	pi.blocks = nil
	pi.links = nil
	pi.userIDMap = nil
	pi.groupIDMap = nil
}

func (pi *T) SetInfoList(entries file.InfoList) {
	pi.entries = entries
	pi.resetDerived()
}

func (pi *T) AppendInfoList(entries file.InfoList) {
	pi.entries = append(pi.entries, entries...)
	pi.resetDerived()
}

func (pi *T) AppendInfo(entry file.Info) {
	pi.entries = append(pi.entries, entry)
	pi.resetDerived()
}

func (pi T) Size() int64 {
	return pi.size
}

func (pi T) Blocks() int64 {
	return pi.xattr.Blocks
}

func (pi T) Mode() fs.FileMode {
	return pi.mode
}

func (pi T) Type() fs.FileMode {
	return pi.mode.Type()
}

func (pi T) ModTime() time.Time {
	return pi.modTime
}

func (pi T) UserGroup() (uid, gid int64) {
	return pi.xattr.UID, pi.xattr.GID
}

func (pi T) DevIno() (device, inode uint64) {
	return pi.xattr.Device, pi.xattr.FileID
}

func (pi T) XAttr() file.XAttr {
	return pi.xattr
}

func (pi T) XAttrInfo(fi file.Info) file.XAttr {
	return pi.xAttrFromSys(fi.Sys())
}

// Info returns the list of file.Info's available for this prefix.
// NOTE that these may contain directories, ie. entries for which
// IsDir is true.
func (pi T) InfoList() file.InfoList {
	return pi.entries
}

func (pi T) Unchanged(npi T) bool {
	return pi.modTime.Equal(npi.modTime) &&
		pi.mode == npi.mode &&
		pi.xattr.UID == npi.xattr.UID &&
		pi.xattr.GID == npi.xattr.GID
}

func (pi T) UnchangedInfo(info file.Info) bool {
	if !pi.modTime.Equal(info.ModTime()) || pi.mode != info.Mode() {
		return false
	}
	switch v := info.Sys().(type) {
	case file.XAttr:
		return pi.xattr.UID == v.UID && pi.xattr.GID == v.GID
	case *file.XAttr:
		if v != nil {
			return pi.xattr.UID == v.UID && pi.xattr.GID == v.GID
		}
	}
	return true
}

func (pi T) FilesOnly() file.InfoList {
	fi := make(file.InfoList, 0, len(pi.entries))
	for _, f := range pi.entries {
		if !f.IsDir() {
			fi = append(fi, f)
		}
	}
	return fi
}

func (pi T) PrefixesOnly() file.InfoList {
	fi := make(file.InfoList, 0, len(pi.entries))
	for _, f := range pi.entries {
		if f.IsDir() {
			fi = append(fi, f)
		}
	}
	return fi
}

func (pi *T) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(1000)
	if err := pi.AppendBinary(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (pi *T) AppendBinary(buf *bytes.Buffer) error {
	if err := pi.finalize(); err != nil {
		return err
	}

	var storage [128]byte
	data := storage[:0]
	data = append(data, 0x3)                          // version
	data = binary.AppendVarint(data, pi.size)         // size
	data = binary.AppendVarint(data, pi.xattr.Blocks) // nblocks
	data = binary.AppendVarint(data, pi.xattr.UID)    // user id
	data = binary.AppendVarint(data, pi.xattr.GID)    // groupd id

	data = binary.LittleEndian.AppendUint32(data, uint32(pi.mode)) // filemode
	out, err := pi.modTime.MarshalBinary()                         // modtime
	if err != nil {
		return err
	}
	data = binary.AppendVarint(data, int64(len(out)))
	data = append(data, out...)
	if _, err := buf.Write(data); err != nil {
		return err
	}

	pi.userIDMap.appendBinary(buf)  // user id map
	pi.groupIDMap.appendBinary(buf) // group id map

	if err := pi.entries.AppendBinary(buf); err != nil { // files+prefixes
		return err
	}

	data = storage[:0]
	data = binary.AppendUvarint(data, pi.xattr.Device) // pi.device
	data = binary.AppendUvarint(data, pi.xattr.FileID) // pi.inode
	for _, ino := range pi.inodes {
		data = binary.AppendUvarint(data, ino) // inodes
	}
	for _, blk := range pi.blocks {
		data = binary.AppendVarint(data, blk) // blocks
	}
	data = pi.appendLinks(data) // link counts, since version 3
	_, err = buf.Write(data)
	return err
}

// defaultLinks returns the link count that is assumed for all of the
// files whose link counts are not explicitly encoded, ie. whichever of 1
// or 0 (unknown) is the more common.
func (pi *T) defaultLinks() uint64 {
	var ones, zeros int
	for i, e := range pi.entries {
		if e.IsDir() {
			continue
		}
		switch pi.links[i] {
		case 0:
			zeros++
		case 1:
			ones++
		}
	}
	if zeros > ones {
		return 0
	}
	return 1
}

// appendLinks encodes the link counts of the files, but not the prefixes,
// as the default count followed by the number of files whose count differs
// from it and the position, relative to the previous one, and count of each
// of those files. Since almost all files typically have a single link this
// requires only a few bytes per prefix.
func (pi *T) appendLinks(data []byte) []byte {
	def := pi.defaultLinks()
	n := 0
	for i, e := range pi.entries {
		if !e.IsDir() && pi.links[i] != def {
			n++
		}
	}
	data = binary.AppendUvarint(data, def)
	data = binary.AppendUvarint(data, uint64(n))
	prev := 0
	for i, e := range pi.entries {
		if e.IsDir() || pi.links[i] == def {
			continue
		}
		data = binary.AppendUvarint(data, uint64(i-prev))
		data = binary.AppendUvarint(data, pi.links[i])
		prev = i
	}
	return data
}

// decodeLinks decodes the link counts encoded by appendLinks.
func (pi *T) decodeLinks(data []byte) error {
	def, data, err := readUvarint(data)
	if err != nil {
		return fmt.Errorf("PrefixInfo: default links: %w", err)
	}
	n, data, err := readUvarint(data)
	if err != nil {
		return fmt.Errorf("PrefixInfo: number of links: %w", err)
	}
	if n > uint64(len(pi.entries)) {
		return fmt.Errorf("PrefixInfo: invalid number of links %d (entries %d)", n, len(pi.entries))
	}
	for i, e := range pi.entries {
		if !e.IsDir() {
			pi.links[i] = def
		}
	}
	pos := uint64(0)
	for j := range n {
		var delta, links uint64
		if delta, data, err = readUvarint(data); err != nil {
			return fmt.Errorf("PrefixInfo: link[%d] position: %w", j, err)
		}
		if links, data, err = readUvarint(data); err != nil {
			return fmt.Errorf("PrefixInfo: link[%d]: %w", j, err)
		}
		if (j > 0 && delta == 0) || delta >= uint64(len(pi.entries))-pos {
			return fmt.Errorf("PrefixInfo: link[%d]: invalid position delta %d (pos %d)", j, delta, pos)
		}
		pos += delta
		if pi.entries[pos].IsDir() {
			return fmt.Errorf("PrefixInfo: link[%d]: invalid position %d", j, pos)
		}
		pi.links[pos] = links
	}
	if len(data) != 0 {
		return fmt.Errorf("PrefixInfo: trailing link data (%d bytes)", len(data))
	}
	return nil
}

func decodeBinaryInfoListSafe(data []byte) (entries file.InfoList, rest []byte, err error) {
	l, rem, err := readVarint(data)
	if err != nil {
		return nil, nil, fmt.Errorf("PrefixInfo: entries count: %w", err)
	}
	// Each encoded file.Info requires at least 8 bytes (version, name len, size, mode, modTime len).
	if l < 0 || uint64(l) > uint64(len(rem))/8 {
		return nil, nil, fmt.Errorf("PrefixInfo: invalid entries count %d (remaining %d)", l, len(rem))
	}
	defer func() {
		if r := recover(); r != nil {
			entries, rest, err = nil, nil, fmt.Errorf("PrefixInfo: corrupt entry list: %v", r)
		}
	}()
	return file.DecodeBinaryInfoList(data)
}

func (pi *T) decodeHeader(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("PrefixInfo: insufficient data")
	}
	version := data[0]
	if version < 0x1 || version > 0x3 {
		return nil, fmt.Errorf("PrefixInfo: invalid version of binary encoding: got %x, want %x..%x", data[0], 01, 03)
	}
	data = data[1:]

	var err error
	for _, dst := range []*int64{&pi.size, &pi.xattr.Blocks, &pi.xattr.UID, &pi.xattr.GID} {
		if *dst, data, err = readVarint(data); err != nil {
			return nil, fmt.Errorf("PrefixInfo: header: %w", err)
		}
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("PrefixInfo: insufficient data for mode")
	}
	pi.mode = fs.FileMode(binary.LittleEndian.Uint32(data))
	data = data[4:]

	ts, data, err := readVarint(data)
	if err != nil {
		return nil, fmt.Errorf("PrefixInfo: modTime length: %w", err)
	}
	if ts < 0 || ts > int64(len(data)) {
		return nil, fmt.Errorf("PrefixInfo: invalid modTime length %d (remaining %d)", ts, len(data))
	}
	if err := pi.modTime.UnmarshalBinary(data[:ts]); err != nil {
		return nil, err
	}
	return data[ts:], nil
}

func (pi *T) decodeEntriesAndInodes(version byte, data []byte) error {
	var err error
	if data, err = pi.userIDMap.decodeBinary(data); err != nil {
		return err
	}
	if data, err = pi.groupIDMap.decodeBinary(data); err != nil {
		return err
	}
	if pi.entries, data, err = decodeBinaryInfoListSafe(data); err != nil {
		return err
	}
	if pi.xattr.Device, data, err = readUvarint(data); err != nil {
		return fmt.Errorf("PrefixInfo: device: %w", err)
	}
	if pi.xattr.FileID, data, err = readUvarint(data); err != nil {
		return fmt.Errorf("PrefixInfo: fileID: %w", err)
	}
	pi.inodes = make([]uint64, len(pi.entries))
	for i := range pi.inodes {
		if pi.inodes[i], data, err = readUvarint(data); err != nil {
			return fmt.Errorf("PrefixInfo: inode[%d]: %w", i, err)
		}
	}
	pi.blocks = make([]int64, len(pi.entries))
	for i := range pi.blocks {
		if pi.blocks[i], data, err = readVarint(data); err != nil {
			return fmt.Errorf("PrefixInfo: block[%d]: %w", i, err)
		}
	}
	// Link counts are unknown, ie. zero, prior to version 3.
	pi.links = make([]uint64, len(pi.entries))
	if version < 0x3 {
		return nil
	}
	return pi.decodeLinks(data)
}

func (pi *T) UnmarshalBinary(data []byte) error {
	pi.resetDerived()
	if len(data) == 0 {
		return fmt.Errorf("PrefixInfo: insufficient data")
	}
	version := data[0]
	data, err := pi.decodeHeader(data)
	if err != nil {
		return err
	}
	if err := pi.decodeEntriesAndInodes(version, data); err != nil {
		return err
	}
	return pi.finalizeOnUnmarshal()
}

func newIDMapIfNeeded(idms *idMaps, id int64, n int) int {
	if mi := idms.idMapFor(id); mi >= 0 {
		return mi
	}
	*idms = append(*idms, newIDMap(id, n))
	return len(*idms) - 1
}

func (pi *T) createIDMapsAndInodes() error {
	prefixUserMap := newIDMap(pi.xattr.UID, len(pi.entries))
	prefixGroupMap := newIDMap(pi.xattr.GID, len(pi.entries))

	pi.inodes = make([]uint64, len(pi.entries))
	pi.blocks = make([]int64, len(pi.entries))
	pi.links = make([]uint64, len(pi.entries))
	for i, file := range pi.entries {
		xattr := pi.xAttrFromSys(file.Sys())
		if pi.xattr.UID == xattr.UID {
			prefixUserMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.userIDMap, xattr.UID, len(pi.entries))
			pi.userIDMap[mi].set(i)
		}
		if pi.xattr.GID == xattr.GID {
			prefixGroupMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.groupIDMap, xattr.GID, len(pi.entries))
			pi.groupIDMap[mi].set(i)
		}
		pi.inodes[i] = xattr.FileID
		pi.blocks[i] = xattr.Blocks
		if !file.IsDir() {
			// The link count of a directory reflects its sub-directories
			// rather than any hardlinks to it and hence is not recorded.
			pi.links[i] = xattr.Hardlinks
		}
	}

	if len(pi.userIDMap) > 0 {
		pi.userIDMap = append([]idMap{prefixUserMap}, pi.userIDMap...)
	}
	if len(pi.groupIDMap) > 0 {
		pi.groupIDMap = append([]idMap{prefixGroupMap}, pi.groupIDMap...)
	}
	return nil
}

func (pi *T) validateSingleIDMaps(idms idMaps) error {
	ids := map[int64]struct{}{}
	for _, idm := range idms {
		if _, ok := ids[idm.ID]; ok {
			return fmt.Errorf("duplicate id: %v", idm.ID)
		}
		ids[idm.ID] = struct{}{}
	}
	return nil
}

func (pi *T) validateIDMaps() error {
	if pi.userIDMap == nil && pi.groupIDMap == nil {
		return nil
	}
	if err := pi.validateSingleIDMaps(pi.userIDMap); err != nil {
		return fmt.Errorf("user id maps: %v", err)
	}
	if err := pi.validateSingleIDMaps(pi.groupIDMap); err != nil {
		return fmt.Errorf("group id maps: %v", err)
	}
	for i := range pi.entries {
		if pi.userIDMap != nil {
			if _, ok := pi.userIDMap.idForPos(i); !ok {
				return fmt.Errorf("missing user id for file %v", i)
			}
		}
		if pi.groupIDMap != nil {
			if _, ok := pi.groupIDMap.idForPos(i); !ok {
				return fmt.Errorf("missing group id for file %v", i)
			}
		}
	}
	return nil
}

func (pi *T) fsOnly(i int) fsOnly {
	return fsOnly{ino: pi.inodes[i], blocks: pi.blocks[i], links: pi.links[i]}
}

func (pi *T) finalizePerFileInfo() {
	if len(pi.userIDMap) == 0 && len(pi.groupIDMap) == 0 {
		// All files have the same info as the prefix.
		for i := range pi.entries {
			(&pi.entries[i]).SetSys(pi.fsOnly(i))
		}
		return
	}

	for i := range pi.entries {
		uid, gid := pi.xattr.UID, pi.xattr.GID
		if len(pi.userIDMap) > 0 {
			uid, _ = pi.userIDMap.idForPos(i)
		}
		if len(pi.groupIDMap) > 0 {
			gid, _ = pi.groupIDMap.idForPos(i)
		}
		(&pi.entries[i]).SetSys(idAndFS{
			uid: uid, gid: gid, fsOnly: pi.fsOnly(i)})
	}

}

func (pi *T) finalize() error {
	if pi.finalized {
		return nil
	}
	if err := pi.createIDMapsAndInodes(); err != nil {
		return err
	}
	pi.finalized = true
	return pi.validateIDMaps()
}

func (pi *T) finalizeOnUnmarshal() error {
	if err := pi.validateIDMaps(); err != nil {
		return err
	}
	pi.finalizePerFileInfo()
	pi.finalized = true
	return nil
}

// TODO(cnicolaou): parametize for posix and non-posix (ie. numeric vs string) UID/GID.

// IDScanner allows for iterating over files that belong to a particular user
// or group.
type IDSanner interface {
	Next() bool
	Info() file.Info
}

type nullScanner struct {
	n       int
	i       file.Info
	entries file.InfoList
}

func (s *nullScanner) Next() bool {
	if s.n >= len(s.entries) {
		return false
	}
	s.i = s.entries[s.n]
	s.n++
	return true
}

func (s *nullScanner) Info() file.Info {
	return s.i
}

type idmapScanner struct {
	sc      *idMapScanner
	entries file.InfoList
}

func (s *idmapScanner) Next() bool {
	return s.sc.next()
}

func (s *idmapScanner) Info() file.Info {
	return s.entries[s.sc.pos()]
}

// UserIDScan returns an IDSanner for the supplied user id. It can
// only be used after Finalize has been called.
func (pi *T) UserIDScan(id int64) (IDSanner, error) {
	return pi.newIDScan(id, true, pi.userIDMap)
}

// GroupIDScan returns an IDSanner for the supplied group id.
func (pi *T) GroupIDScan(id int64) (IDSanner, error) {
	return pi.newIDScan(id, false, pi.groupIDMap)
}

func (pi *T) newIDScan(id int64, userID bool, idms idMaps) (IDSanner, error) {
	if !pi.finalized {
		return nil, fmt.Errorf("prefix info not finalized")
	}
	idm := idms.idMapFor(id)
	if idm < 0 {
		if userID && id == pi.xattr.UID {
			return &nullScanner{entries: pi.entries}, nil
		}
		if !userID && id == pi.xattr.GID {
			return &nullScanner{entries: pi.entries}, nil
		}
		if userID {
			return nil, fmt.Errorf("no such user id: %v", id)
		}
		return nil, fmt.Errorf("no such group id: %v", id)
	}
	return &idmapScanner{sc: newIDMapScanner(idms[idm]), entries: pi.entries}, nil
}
