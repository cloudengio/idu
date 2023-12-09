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

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

// T represents the information for a prefix, ie. a directory.
// It is primarily intended to be stored in a key/value store or database
// and hence attention is paid to minizing its storage requirements.
// In particular, the name of the prefix is not stored as it is assumed
// to be implicitly known from the store's key or database entry.
type T struct {
	userID     uint32
	groupID    uint32
	device     uint64
	inode      uint64
	size       int64
	mode       fs.FileMode
	modTime    time.Time
	entries    file.InfoList // files and prefixes only
	inodes     []uint64
	userIDMap  idMaps
	groupIDMap idMaps
	finalized  bool
}

// New creates a new PrefixInfo for the supplied file.Info. It will
// determine the uid, gid, device and inode from the supplied file.Info assuming
// that it was created by a call to LStat or Stat rather than being obtained
// from the database.
func New(info file.Info) (T, error) {
	uid, gid, dev, ino, ok := getSysInfo(info)
	if !ok {
		return T{}, fmt.Errorf("no system available info for %v", info.Name())
	}
	return T{
		userID:  uid,
		groupID: gid,
		device:  dev,
		inode:   ino,
		size:    info.Size(),
		modTime: info.ModTime(),
		mode:    info.Mode(),
	}, nil
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

func (pi T) Mode() fs.FileMode {
	return pi.mode
}

func (pi T) ModTime() time.Time {
	return pi.modTime
}

func (pi T) UserGroup() (uid, gid uint32) {
	return pi.userID, pi.groupID
}

func (pi T) DevIno() (device, inode uint64) {
	return pi.device, pi.inode
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
	data = append(data, 0x2)                              // version
	data = binary.AppendVarint(data, pi.size)             // user id
	data = binary.AppendUvarint(data, uint64(pi.userID))  // user id
	data = binary.AppendUvarint(data, uint64(pi.groupID)) // groupd id

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
	data = binary.AppendUvarint(data, pi.device) // pi.device
	data = binary.AppendUvarint(data, pi.inode)  // pi.inode
	for _, ino := range pi.inodes {
		data = binary.AppendUvarint(data, ino) // inodes
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

	uid, n := binary.Uvarint(data) // userid
	data = data[n:]
	gid, n := binary.Uvarint(data) // groupid
	data = data[n:]
	pi.userID, pi.groupID = uint32(uid), uint32(gid)

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
	if version >= 0x2 {
		pi.device, n = binary.Uvarint(data)
		data = data[n:]
		pi.inode, n = binary.Uvarint(data)
		data = data[n:]
		pi.inodes = make([]uint64, len(pi.entries))
		for i := range pi.inodes {
			pi.inodes[i], n = binary.Uvarint(data)
			data = data[n:]
		}
	}
	return pi.finalizeOnUnmarshal()
}

func newIDMapIfNeeded(idms *idMaps, id uint32, n int) int {
	if mi := idms.idMapFor(id); mi >= 0 {
		return mi
	}
	*idms = append(*idms, newIDMap(id, n))
	return len(*idms) - 1
}

func (pi *T) createIDMapsEtc() {
	prefixUserMap := newIDMap(pi.userID, len(pi.entries))
	prefixGroupMap := newIDMap(pi.groupID, len(pi.entries))

	pi.inodes = make([]uint64, len(pi.entries))
	for i, file := range pi.entries {
		uid, gid, _, ino := pi.SysInfo(file)
		if pi.userID == uid {
			prefixUserMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.userIDMap, uid, len(pi.entries))
			pi.userIDMap[mi].set(i)
		}
		if pi.groupID == gid {
			prefixGroupMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.groupIDMap, gid, len(pi.entries))
			pi.groupIDMap[mi].set(i)
		}
		pi.inodes[i] = ino
	}

	if len(pi.userIDMap) > 0 {
		pi.userIDMap = append([]idMap{prefixUserMap}, pi.userIDMap...)
	}
	if len(pi.groupIDMap) > 0 {
		pi.groupIDMap = append([]idMap{prefixGroupMap}, pi.groupIDMap...)
	}
}

func (pi *T) validateSingleIDMaps(idms idMaps) error {
	ids := map[uint32]struct{}{}
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
			(&pi.entries[i]).SetSys(inoOnly(pi.inodes[i]))
		}
		return
	}
	for i := range pi.entries {
		uid, gid := pi.userID, pi.groupID
		if len(pi.userIDMap) > 0 {
			uid, _ = pi.userIDMap.idForPos(i)
		}
		if len(pi.groupIDMap) > 0 {
			gid, _ = pi.groupIDMap.idForPos(i)
		}
		(&pi.entries[i]).SetSys(idAndIno{
			uid: uid, gid: gid, ino: pi.inodes[i]})
	}
}

func (pi *T) finalize() error {
	if pi.finalized {
		return nil
	}
	pi.createIDMapsEtc()
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

// ComputeStats computes all available statistics for this Prefix, including
// using the supplied calculator to determine on-disk raw storage usage.
// Note that the size of the prefix itself is not included in the returned
// PrefixBytes but rather is included in the PrefixBytes for its parent prefix.
func (pi *T) ComputeStats(calculator diskusage.Calculator, expr boolexpr.T) (totals Stats, userStats, groupStats StatsList, err error) {
	pi.finalize()
	userStats = pi.computeStatsForIDMapOrFiles(pi.userIDMap, pi.userID, calculator, expr)
	groupStats = pi.computeStatsForIDMapOrFiles(pi.groupIDMap, pi.groupID, calculator, expr)
	for _, us := range userStats {
		totals.Bytes += us.Bytes
		totals.Files += us.Files
		totals.Prefixes += us.Prefixes
		totals.PrefixBytes += us.PrefixBytes
		totals.StorageBytes += us.StorageBytes
	}
	return
}

func (pi *T) computeStatsForIDMapOrFiles(idms idMaps, defaultID uint32, calculator diskusage.Calculator, expr boolexpr.T) []Stats {
	if len(idms) == 0 {
		var stats Stats
		stats.ID = defaultID
		var found bool
		for _, fi := range pi.entries {
			found = pi.updateStats(&stats, fi, calculator, expr)
		}
		if !found {
			return nil
		}
		return []Stats{stats}
	}
	stats := make([]Stats, 0, len(idms))
	for _, idm := range idms {
		if s, ok := pi.computeStatsForID(idm, calculator, expr); ok {
			stats = append(stats, s)
		}
	}
	return stats
}

func (pi *T) updateStats(s *Stats, fi file.Info, calculator diskusage.Calculator, expr boolexpr.T) bool {
	uid, gid, _, _ := pi.SysInfo(fi)
	if !expr.Eval(boolexpr.NewFileInfoUserGroup(fi, uid, gid)) {
		return false
	}
	if fi.IsDir() {
		s.Prefixes++
		s.PrefixBytes += fi.Size()
	} else {
		s.Files++
		s.StorageBytes += calculator.Calculate(fi.Size())
		s.Bytes += fi.Size()
	}
	return true
}

func (pi *T) computeStatsForID(idm idMap, calculator diskusage.Calculator, expr boolexpr.T) (Stats, bool) {
	var stats Stats
	stats.ID = idm.ID
	sc := newIdMapScanner(idm)
	found := false
	for sc.next() {
		fi := pi.entries[sc.pos()]
		if pi.updateStats(&stats, fi, calculator, expr) {
			found = true
		}
	}
	return stats, found
}

// TODO(cnicolaou): get rid of this?

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
func (pi *T) UserIDScan(id uint32) (IDSanner, error) {
	return pi.newIDScan(id, true, pi.userIDMap)
}

// GroupIDScan returns an IDSanner for the supplied group id.
func (pi *T) GroupIDScan(id uint32) (IDSanner, error) {
	return pi.newIDScan(id, false, pi.groupIDMap)
}

func (pi *T) newIDScan(id uint32, userID bool, idms idMaps) (IDSanner, error) {
	if !pi.finalized {
		return nil, fmt.Errorf("prefix info not finalized")
	}
	idm := idms.idMapFor(id)
	if idm < 0 {
		if userID && id == pi.userID {
			return &nullScanner{entries: pi.entries}, nil
		}
		if !userID && id == pi.groupID {
			return &nullScanner{entries: pi.entries}, nil
		}
		if userID {
			return nil, fmt.Errorf("no such user id: %v", id)
		}
		return nil, fmt.Errorf("no such group id: %v", id)
	}
	return &idmapScanner{sc: newIdMapScanner(idms[idm]), entries: pi.entries}, nil
}

// Named represents a PrefixInfo that has a name associated with it
// and also a NumEntries() method that returns the number of entries
// in the prefix.
type Named struct {
	T
	name string
}

func (pi Named) Name() string {
	return pi.name
}

func (pi Named) NumEntries() int64 {
	return int64(len(pi.entries))
}

func NewNamed(name string, pi T) Named {
	return Named{name: name, T: pi}
}
