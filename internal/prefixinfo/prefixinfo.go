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

func (pi *T) SetInfoList(entries file.InfoList) {
	pi.entries = entries
}

func (pi *T) AppendInfoList(entries file.InfoList) {
	pi.entries = append(pi.entries, entries...)
}

func (pi *T) AppendInfo(entry file.Info) {
	pi.entries = append(pi.entries, entry)
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
	return pi.modTime.Equal(npi.modTime) && pi.mode == npi.mode
}

func (pi T) UnchangedInfo(info file.Info) bool {
	return pi.modTime.Equal(info.ModTime()) && pi.mode == info.Mode()
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
	data = append(data, 0x2)                          // version
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
	_, err = buf.Write(data)
	return err
}

func (pi *T) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("PrefixInfo: insufficient data")
	}
	version := data[0]

	if version != 0x1 && version != 0x2 {
		return fmt.Errorf("PrefixInfo: invalid version of binary encoding: got %x, want %x..%x", data[0], 01, 02)
	}
	var n int
	data = data[1:]                  // version
	pi.size, n = binary.Varint(data) // size
	data = data[n:]

	pi.xattr.Blocks, n = binary.Varint(data) // nblocks
	data = data[n:]

	uid, n := binary.Varint(data) // userid
	data = data[n:]

	gid, n := binary.Varint(data) // groupid
	data = data[n:]
	pi.xattr.UID, pi.xattr.GID = uid, gid

	pi.mode = fs.FileMode(binary.LittleEndian.Uint32(data)) // filemode
	data = data[4:]
	ts, n := binary.Varint(data) // modtime
	data = data[n:]
	if err := pi.modTime.UnmarshalBinary(data[0:ts]); err != nil { // time
		return err
	}
	data = data[ts:]

	var err error

	data, err = pi.userIDMap.decodeBinary(data) // user id maps
	if err != nil {
		return err
	}
	data, err = pi.groupIDMap.decodeBinary(data) // group id maps
	if err != nil {
		return err
	}
	pi.entries, data, err = file.DecodeBinaryInfoList(data) // files
	if err != nil {
		return err
	}
	pi.xattr.Device, n = binary.Uvarint(data)
	data = data[n:]
	pi.xattr.FileID, n = binary.Uvarint(data)
	data = data[n:]
	pi.inodes = make([]uint64, len(pi.entries))
	for i := range pi.inodes {
		pi.inodes[i], n = binary.Uvarint(data)
		data = data[n:]
	}
	pi.blocks = make([]int64, len(pi.entries))
	for i := range pi.blocks {
		pi.blocks[i], n = binary.Varint(data)
		data = data[n:]
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

func (pi *T) finalizePerFileInfo() {
	if len(pi.userIDMap) == 0 && len(pi.groupIDMap) == 0 {
		// All files have the same info as the prefix.
		for i := range pi.entries {
			(&pi.entries[i]).SetSys(fsOnly{pi.inodes[i], pi.blocks[i]})
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
			uid: uid, gid: gid, fsOnly: fsOnly{pi.inodes[i], pi.blocks[i]}})
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
