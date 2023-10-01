// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"encoding/binary"
	"math/bits"

	"cloudeng.io/file"
)

type idMap struct {
	UserID, GroupID uint32
	Pos             []uint64
	bitPos          int
}

func (idm idMap) appendBinary(data []byte) ([]byte, error) {
	data = binary.AppendUvarint(data, uint64(idm.UserID))
	data = binary.AppendUvarint(data, uint64(idm.GroupID))
	data = binary.AppendUvarint(data, uint64(len(idm.Pos)))
	for _, p := range idm.Pos {
		data = binary.AppendUvarint(data, p)
	}
	return data, nil
}

func (idm *idMap) decodeBinary(data []byte) ([]byte, error) {
	var n int
	uid, n := binary.Uvarint(data)
	data = data[n:]
	gid, n := binary.Uvarint(data)
	data = data[n:]
	idm.UserID, idm.GroupID = uint32(uid), uint32(gid)
	l, n := binary.Uvarint(data)
	data = data[n:]
	idm.Pos = make([]uint64, l)
	for i := range idm.Pos {
		idm.Pos[i], n = binary.Uvarint(data)
		data = data[n:]
	}
	return data, nil
}

type idMaps []idMap

func (idms idMaps) appendBinary(data []byte) ([]byte, error) {
	data = binary.AppendUvarint(data, uint64(len(idms)))
	for _, idm := range idms {
		data, _ = idm.appendBinary(data)
	}
	return data, nil
}

func (idms *idMaps) decodeBinary(data []byte) ([]byte, error) {
	var n int
	l, n := binary.Uvarint(data)
	data = data[n:]
	*idms = make([]idMap, l)
	for i := range *idms {
		data, _ = (*idms)[i].decodeBinary(data)
	}
	return data, nil
}

func (idms idMaps) idMapFor(uid, gid uint32) int {
	for i, idm := range idms {
		if idm.UserID == uid && idm.GroupID == gid {
			return i
		}
	}
	return -1
}

func newIDMap(uid, gid uint32, n int) idMap {
	return idMap{
		UserID:  uid,
		GroupID: gid,
		Pos:     make([]uint64, n/64+1),
	}
}

func (idm *idMap) set(pos int) {
	idm.Pos[pos/64] |= 1 << (pos % 64)
}

func (idm *idMap) isSet(pos int) bool {
	return idm.Pos[pos/64]>>(pos%64)&1 == 1
}

func newIdMapScanner(idm idMap) *idMapScanner {
	return &idMapScanner{
		idMap:  idm,
		bitPos: -1,
	}
}

type idMapScanner struct {
	idMap
	bitPos  int
	started bool
}

func select1(shift int, words []uint64) int {
	for i, w := range words {
		w >>= shift
		if w != 0 {
			return i*64 + bits.TrailingZeros64(w) + shift
		}
		shift = 0
	}
	return -1
}

func (idm *idMapScanner) next() bool {
	if idm.bitPos == -1 {
		idm.bitPos = select1(0, idm.Pos)
		return idm.bitPos >= 0
	}
	idm.bitPos++
	word := idm.bitPos / 64
	bit := idm.bitPos % 64
	idm.bitPos = select1(bit, idm.Pos[word:])
	if idm.bitPos < 0 {
		return false
	}
	idm.bitPos += word * 64
	return true
}

func (idm *idMapScanner) pos() int {
	return idm.bitPos
}

func (idm *idMap) computeFileStats(files file.InfoList, storageBytes func(uint64) uint64) (userStats, groupStats Stats) {
	userStats.ID, groupStats.ID = idm.UserID, idm.GroupID
	sc := newIdMapScanner(*idm)
	for sc.next() {
		pos := sc.pos()
		fi := files[pos]
		userStats.Files++
		userStats.Bytes += uint64(fi.Size())

	}
	return
}
