// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"strings"
)

// idMap is a bit map of file positions for a given id. They are used
// to encode/decode user/group information in a space efficient manner.
type idMap struct {
	ID  uint32
	Pos []uint64
}

func (idm idMap) String() string {
	return fmt.Sprintf("% 3d:%064b", idm.ID, idm.Pos)
}

func (idm idMap) appendBinary(buf *bytes.Buffer) {
	var storage [16]byte
	data := storage[:0]
	data = binary.AppendUvarint(data, uint64(idm.ID))
	data = binary.AppendUvarint(data, uint64(len(idm.Pos)))
	buf.Write(data)
	for _, p := range idm.Pos {
		n := binary.PutUvarint(storage[:], p)
		buf.Write(storage[:n])
	}
	return
}

func (idm *idMap) decodeBinary(data []byte) ([]byte, error) {
	uid, n := binary.Uvarint(data)
	data = data[n:]
	idm.ID = uint32(uid)
	l, n := binary.Uvarint(data)
	data = data[n:]
	if l > 0 {
		idm.Pos = make([]uint64, l)
		for i := range idm.Pos {
			idm.Pos[i], n = binary.Uvarint(data)
			data = data[n:]
		}
	}
	return data, nil
}

type idMaps []idMap

func (idms idMaps) String() string {
	out := strings.Builder{}
	out.WriteString("[\n")
	for _, idm := range idms {
		out.WriteRune('\t')
		out.WriteString(idm.String())
		out.WriteRune('\n')
	}
	out.WriteString("]\n")
	return out.String()
}

func (idms idMaps) appendBinary(buf *bytes.Buffer) {
	var storage [5]byte
	n := binary.PutUvarint(storage[:], uint64(len(idms)))
	buf.Write(storage[:n])
	for _, idm := range idms {
		idm.appendBinary(buf)
	}
	return
}

func (idms *idMaps) decodeBinary(data []byte) ([]byte, error) {
	var n int
	l, n := binary.Uvarint(data)
	data = data[n:]
	if l > 0 {
		*idms = make([]idMap, l)
		for i := range *idms {
			data, _ = (*idms)[i].decodeBinary(data)
		}
	}
	return data, nil
}

func (idms idMaps) idMapFor(id uint32) int {
	for i, idm := range idms {
		if idm.ID == id {
			return i
		}
	}
	return -1
}

func (idms idMaps) idForPos(pos int) (uint32, bool) {
	for _, idm := range idms {
		if idm.isSet(pos) {
			return idm.ID, true
		}
	}
	return 0, false
}

func newIDMap(id uint32, n int) idMap {
	return idMap{
		ID:  id,
		Pos: make([]uint64, n/64+1),
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
	bitPos int
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
