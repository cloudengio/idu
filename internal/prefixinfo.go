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
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/filewalk"
)

type PrefixInfo struct {
	UserID        uint32
	GroupID       uint32
	Size          uint64
	Mode          fs.FileMode
	ModTime       time.Time
	Children      filewalk.EntryList
	Files         file.InfoList
	userIDMap     idMaps
	groupIDMap    idMaps
	idMapsCreated bool
}

func (pi *PrefixInfo) MarshalBinary() (data []byte, err error) {
	pi.createIDMaps()

	data = make([]byte, 0, 100)
	data = append(data, 0x1) // version

	data = binary.AppendUvarint(data, pi.Size)            // user id
	data = binary.AppendUvarint(data, uint64(pi.UserID))  // user id
	data = binary.AppendUvarint(data, uint64(pi.GroupID)) // groupd id

	data = binary.LittleEndian.AppendUint32(data, uint32(pi.Mode)) // filemode
	out, err := pi.ModTime.MarshalBinary()                         // modtime
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
	data = data[1:]                   // version
	pi.Size, n = binary.Uvarint(data) // size
	data = data[n:]

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

	var err error

	data, err = pi.userIDMap.decodeBinary(data) // user id maps
	if err != nil {
		return err
	}
	data, err = pi.groupIDMap.decodeBinary(data) // group id maps
	if err != nil {
		return err
	}
	pi.idMapsCreated = true

	data, err = pi.Children.DecodeBinary(data) // children
	if err != nil {
		return err
	}
	pi.Files, _, err = file.DecodeBinaryInfoList(data) // files
	if err != nil {
		return err
	}

	return err
}

func newIDMapIfNeeded(idms *idMaps, id uint32, n int) int {
	if mi := idms.idMapFor(id); mi >= 0 {
		return mi
	}
	*idms = append(*idms, newIDMap(id, n))
	return len(*idms) - 1
}

func (pi *PrefixInfo) createIDMaps() {
	if pi.idMapsCreated {
		return
	}
	prefixUserMap := newIDMap(pi.UserID, len(pi.Files))
	prefixGroupMap := newIDMap(pi.GroupID, len(pi.Files))

	for i, file := range pi.Files {
		uid, gid := pi.GetUserGroup(file)
		if pi.UserID == uid {
			prefixUserMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.userIDMap, uid, len(pi.Files))
			pi.userIDMap[mi].set(i)
		}
		if pi.GroupID == gid {
			prefixGroupMap.set(i)
		} else {
			mi := newIDMapIfNeeded(&pi.groupIDMap, gid, len(pi.Files))
			pi.groupIDMap[mi].set(i)
		}
	}
	if len(pi.userIDMap) > 0 {
		pi.userIDMap = append([]idMap{prefixUserMap}, pi.userIDMap...)
	}

	if len(pi.groupIDMap) > 0 {
		pi.groupIDMap = append([]idMap{prefixGroupMap}, pi.groupIDMap...)
	}
	pi.idMapsCreated = true
}

func (pi *PrefixInfo) ComputeStats(calculator diskusage.Calculator) (userStats, groupStats StatsList) {
	pi.createIDMaps()

	userStats = make([]Stats, len(pi.userIDMap))
	groupStats = make([]Stats, len(pi.groupIDMap))

	for i, idm := range pi.userIDMap {
		userStats[i] = pi.computeStatsForID(idm, calculator)
	}

	for i, idm := range pi.groupIDMap {
		groupStats[i] = pi.computeStatsForID(idm, calculator)
	}
	return
}

func (pi *PrefixInfo) computeStatsForID(idm idMap, calculator diskusage.Calculator) Stats {
	var stats Stats
	sc := newIdMapScanner(idm)
	for sc.next() {
		fi := pi.Files[sc.pos()]
		stats.Files++
		stats.Bytes += fi.Size()
		stats.StorageBytes += calculator.Calculate(fi.Size())
	}
	return stats
}

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

func (pi *PrefixInfo) UserIDScan(id uint32) (IDSanner, error) {
	return pi.newIDScan(id, true, pi.userIDMap)
}

func (pi *PrefixInfo) GroupIDScan(id uint32) (IDSanner, error) {
	return pi.newIDScan(id, false, pi.groupIDMap)
}

func (pi *PrefixInfo) newIDScan(id uint32, userID bool, idms idMaps) (IDSanner, error) {
	pi.createIDMaps()
	idm := idms.idMapFor(id)
	if idm < 0 {
		if userID && id == pi.UserID {
			return &nullScanner{files: pi.Files}, nil
		}
		if !userID && id == pi.GroupID {
			return &nullScanner{files: pi.Files}, nil
		}
		if userID {
			return nil, fmt.Errorf("no such user id: %v", id)
		}
		return nil, fmt.Errorf("no such group id: %v", id)
	}
	return &idmapScanner{sc: newIdMapScanner(idms[idm]), files: pi.Files}, nil
}
