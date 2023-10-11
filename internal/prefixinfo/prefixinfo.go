// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"time"

	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/filewalk"
)

// T represents the information for a prefix, ie. a directory.
// It is primarily intended to be stored in a key/value store or database
// and hence attention is paid to minizing its storage requirements.
// In particular, the name of the prefix is not stored as it is assumed
// to be implicitly known from the store's key or database entry.
type T struct {
	userID     uint32
	groupID    uint32
	size       int64
	mode       fs.FileMode
	modTime    time.Time
	children   filewalk.EntryList
	files      file.InfoList // may include directories also.
	userIDMap  idMaps
	groupIDMap idMaps
	finalized  bool
}

// New creates a new PrefixInfo for the supplied file.Info.
func New(info file.Info) (T, error) {
	uid, gid, ok := UserGroup(info)
	if !ok {
		return T{}, fmt.Errorf("no user/group info for %v", info.Name())
	}
	return T{
		userID:  uid,
		groupID: gid,
		size:    info.Size(),
		modTime: info.ModTime(),
		mode:    info.Mode(),
	}, nil
}

func (pi *T) AppendFiles(files file.InfoList) {
	pi.files = append(pi.files, files...)
}

func (pi *T) AppendInfo(file file.Info) {
	pi.files = append(pi.files, file)
}

func (pi *T) AppendEntries(entries filewalk.EntryList) {
	pi.children = append(pi.children, entries...)
}

func (pi *T) AppendEntry(entry filewalk.Entry) {
	pi.children = append(pi.children, entry)
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

func (pi T) Files() file.InfoList {
	return pi.files
}

func (pi T) Entries() filewalk.EntryList {
	return pi.children
}

func (pi T) Unchanged(npi T) bool {
	return pi.modTime.Equal(npi.modTime) && pi.mode == npi.mode
}

func (pi T) UnchangedInfo(info file.Info) bool {
	return pi.modTime.Equal(info.ModTime()) && pi.mode == info.Mode()
}

func (pi *T) MarshalBinary() (data []byte, err error) {
	if !pi.finalized {
		return nil, fmt.Errorf("prefix info not finalized")
	}

	data = make([]byte, 0, 100)
	data = append(data, 0x1) // version

	data = binary.AppendVarint(data, pi.size)             // user id
	data = binary.AppendUvarint(data, uint64(pi.userID))  // user id
	data = binary.AppendUvarint(data, uint64(pi.groupID)) // groupd id

	data = binary.LittleEndian.AppendUint32(data, uint32(pi.mode)) // filemode
	out, err := pi.modTime.MarshalBinary()                         // modtime
	if err != nil {
		return nil, err
	}
	data = binary.AppendVarint(data, int64(len(out)))
	data = append(data, out...)

	data, err = pi.userIDMap.appendBinary(data) // user id map
	if err != nil {
		return nil, err
	}

	data, err = pi.groupIDMap.appendBinary(data) // group id map
	if err != nil {
		return nil, err
	}

	data, err = pi.children.AppendBinary(data) // children
	if err != nil {
		return nil, err
	}
	return pi.files.AppendBinary(data) // files
}

func (pi *T) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("PrefixInfo: insufficient data")
	}
	if data[0] != 0x1 {
		return fmt.Errorf("PrefixInfo: invalid version of binary encoding: got %x, want %x", data[0], 0x1)
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

	data, err = pi.children.DecodeBinary(data) // children
	if err != nil {
		return err
	}
	pi.files, _, err = file.DecodeBinaryInfoList(data) // files
	if err != nil {
		return err
	}
	return pi.finalize()
}

func newIDMapIfNeeded(idms *idMaps, id uint32, n int) int {
	if mi := idms.idMapFor(id); mi >= 0 {
		return mi
	}
	*idms = append(*idms, newIDMap(id, n))
	return len(*idms) - 1
}

func (pi *T) createIDMaps() {
	prefixUserMap := newIDMap(pi.userID, len(pi.files))
	prefixGroupMap := newIDMap(pi.groupID, len(pi.files))

	for i, file := range pi.files {
		uid, gid := pi.GetUserGroupFile(file)
		if pi.userID == uid {
			prefixUserMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.userIDMap, uid, len(pi.files))
			pi.userIDMap[mi].set(i)
		}
		if pi.groupID == gid {
			prefixGroupMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.groupIDMap, gid, len(pi.files))
			pi.groupIDMap[mi].set(i)
		}
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
	for i := range pi.files {
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

func (pi *T) finalizePerFileUserGroupInfo() {
	if pi.userIDMap == nil && pi.groupIDMap == nil {
		// All files have the same info as the prefix.
		for i := range pi.files {
			(&pi.files[i]).SetSys(nil)
		}
	}
	for i := range pi.files {
		uid, _ := pi.userIDMap.idForPos(i)
		gid, _ := pi.groupIDMap.idForPos(i)
		pi.SetUserGroupFile(&pi.files[i], uid, gid)
	}
	return
}

// Finalize must be called after all files, entries etc have been added to
// the PrefixInfo and will build the per-file user and group mappings.
// Finalize must be called before marshaling a PrefixInfo and consequently
// an unmashaled PrefixInfo will be finalized by the unmarsahling code.
func (pi *T) Finalize() error {
	if pi.finalized {
		return nil
	}
	pi.createIDMaps()
	return pi.finalize()
}

// called by unmarshal to finalize the prefix info but without
// creating new idmaps.
func (pi *T) finalize() error {
	if err := pi.validateIDMaps(); err != nil {
		return err
	}
	pi.finalizePerFileUserGroupInfo()
	pi.finalized = true
	return nil
}

// ComputeStats computes all available statistics for this Prefix, including
// using the supplied calculator to determine on-disk raw storage usage.
func (pi *T) ComputeStats(calculator diskusage.Calculator) (totals Stats, userStats, groupStats StatsList, err error) {
	if !pi.finalized {
		err = fmt.Errorf("prefix info not finalized")
		return
	}
	userStats = pi.computeStatsForIDMapOrFiles(pi.userIDMap, pi.userID, calculator)
	groupStats = pi.computeStatsForIDMapOrFiles(pi.groupIDMap, pi.groupID, calculator)
	for _, us := range userStats {
		totals.Bytes += us.Bytes
		totals.Files += us.Files
		totals.Prefixes += us.Prefixes
		totals.StorageBytes += us.StorageBytes
	}
	return
}

func (pi *T) computeStatsForIDMapOrFiles(idms idMaps, defaultID uint32, calculator diskusage.Calculator) []Stats {
	if idms == nil || len(idms) == 0 {
		var stats Stats
		stats.ID = defaultID
		for _, fi := range pi.files {
			pi.updateStats(&stats, fi, calculator)
		}
		return []Stats{stats}
	}
	stats := make([]Stats, len(idms))
	for i, idm := range idms {
		stats[i] = pi.computeStatsForID(idm, calculator)
	}
	return stats
}

func (pi *T) updateStats(s *Stats, fi file.Info, calculator diskusage.Calculator) {
	if fi.IsDir() {
		s.Prefixes++
		s.StorageBytes += fi.Size()
	} else {
		s.Files++
		s.StorageBytes += calculator.Calculate(fi.Size())
	}
	s.Bytes += fi.Size()
}

func (pi *T) computeStatsForID(idm idMap, calculator diskusage.Calculator) Stats {
	var stats Stats
	sc := newIdMapScanner(idm)
	for sc.next() {
		fi := pi.files[sc.pos()]
		stats.ID = idm.ID
		pi.updateStats(&stats, fi, calculator)
	}
	return stats
}

// IDScanner allows for iterating over files that belong to a particular user
// or group.
type IDSanner interface {
	Next() bool
	Info() file.Info
}

type nullScanner struct {
	n     int
	i     file.Info
	files file.InfoList
}

func (s *nullScanner) Next() bool {
	if s.n >= len(s.files) {
		return false
	}
	s.i = s.files[s.n]
	s.n++
	return true
}

func (s *nullScanner) Info() file.Info {
	return s.i
}

type idmapScanner struct {
	sc    *idMapScanner
	files file.InfoList
}

func (s *idmapScanner) Next() bool {
	return s.sc.next()
}

func (s *idmapScanner) Info() file.Info {
	return s.files[s.sc.pos()]
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
			return &nullScanner{files: pi.files}, nil
		}
		if !userID && id == pi.groupID {
			return &nullScanner{files: pi.files}, nil
		}
		if userID {
			return nil, fmt.Errorf("no such user id: %v", id)
		}
		return nil, fmt.Errorf("no such group id: %v", id)
	}
	return &idmapScanner{sc: newIdMapScanner(idms[idm]), files: pi.files}, nil
}
