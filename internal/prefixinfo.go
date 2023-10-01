// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"time"

	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
)

type PrefixInfo struct {
	UserID     uint32
	GroupID    uint32
	Mode       fs.FileMode
	ModTime    time.Time
	UserStats  StatsList
	GroupStats StatsList
	Children   filewalk.EntryList
	Files      file.InfoList
	idms       idMaps
}

func (pi *PrefixInfo) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0, 100)
	data = append(data, 0x1) // version

	data = binary.AppendUvarint(data, uint64(pi.UserID))  // user id
	data = binary.AppendUvarint(data, uint64(pi.GroupID)) // groupd id

	data = binary.LittleEndian.AppendUint32(data, uint32(pi.Mode)) // filemode
	out, err := pi.ModTime.MarshalBinary()                         // modtime
	if err != nil {
		return nil, err
	}
	data = binary.AppendVarint(data, int64(len(out)))
	data = append(data, out...)

	//data, err = pi.idms.appendBinary(data) // idmaps
	data, err = pi.UserStats.AppendBinary(data) // stats
	if err != nil {
		return nil, err
	}

	data, err = pi.GroupStats.AppendBinary(data) // stats
	if err != nil {
		return nil, err
	}

	data, err = pi.Children.AppendBinary(data) // children
	if err != nil {
		return nil, err
	}
	return pi.Files.AppendBinary(data) // files
}

func (pi *PrefixInfo) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("PrefixInfo: insufficient data")
	}
	if data[0] != 0x1 {
		return fmt.Errorf("PrefixInfo: invalid version of binary encoding: got %x, want %x", data[0], 0x1)
	}
	var n int
	data = data[1:] // version

	uid, n := binary.Uvarint(data) // userid
	data = data[n:]
	gid, n := binary.Uvarint(data) // groupid
	data = data[n:]
	pi.UserID, pi.GroupID = uint32(uid), uint32(gid)

	pi.Mode = fs.FileMode(binary.LittleEndian.Uint32(data)) // filemode
	data = data[4:]
	ts, n := binary.Varint(data) // modtime
	data = data[n:]
	if err := pi.ModTime.UnmarshalBinary(data[0:ts]); err != nil { // time
		return err
	}
	data = data[ts:]

	//data, err = pi.idms.decodeBinary(data) // idmaps
	//if err != nil {
	//	return err
	//}

	var err error

	data, err = pi.UserStats.DecodeBinary(data) // stats
	if err != nil {
		return err
	}
	data, err = pi.GroupStats.DecodeBinary(data) // stats
	if err != nil {
		return err
	}
	data, err = pi.Children.DecodeBinary(data) // children
	if err != nil {
		return err
	}
	pi.Files, data, err = file.DecodeBinaryInfoList(data) // files
	if err != nil {
		return err
	}
	return err
}

func newIDMapIfNeeded(idms *idMaps, uid, gid uint32, n int) int {
	if mi := idms.idMapFor(uid, gid); mi >= 0 {
		return mi
	}
	*idms = append(*idms, newIDMap(uid, gid, n))
	return len(*idms) - 1
}

func (pi *PrefixInfo) createIDMaps() {
	pi.idms = nil
	idmsPrefix := newIDMap(pi.UserID, pi.GroupID, len(pi.Files))
	for i, file := range pi.Files {
		uid, gid, ok := UserInfo(file)
		if !ok {
			continue
		}
		if pi.UserID == uid && pi.GroupID == gid {
			idmsPrefix.set(i)
			continue
		}
		mi := newIDMapIfNeeded(&pi.idms, uid, gid, len(pi.Files))
		pi.idms[mi].set(i)
	}
	if len(pi.idms) > 0 {
		pi.idms = append([]idMap{idmsPrefix}, pi.idms...)
	}
}

func (pi *PrefixInfo) ComputeStats() {
	pi.createIDMaps()

	//	are we double counting here.....

	//	what happens when group id appears twice in two combos - need to separate them
	//	again don't we...

	//	for i := range pi.idms {
	//		userStats, groupStats := pi.computeStatsForUserGroup(i, func(size uint64) uint64 { return size })
	//	}
}

func (pi *PrefixInfo) computeStatsForUserGroup(idx int, storageBytes func(uint64) uint64) (userStats, groupStats Stats) {
	pi.createIDMaps()
	idm := pi.idms[idx]
	sc := newIdMapScanner(idm)
	userStats.ID, groupStats.ID = idm.UserID, idm.GroupID
	for sc.next() {
		pos := sc.pos()
		fi := pi.Files[pos]
		userStats.Files++
		userStats.Bytes += uint64(fi.Size())
		userStats.StorageBytes += storageBytes(uint64(fi.Size()))
	}
	userStats.Dirs = uint64(len(pi.Children))
	groupStats = userStats
	return
}
