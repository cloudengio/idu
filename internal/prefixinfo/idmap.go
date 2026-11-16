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
	ID  int64
	Pos []uint64
}

func (idm idMap) String() string {
	return fmt.Sprintf("% 3d:%064b", idm.ID, idm.Pos)
}

func (idm idMap) appendBinary(buf *bytes.Buffer) {
	var storage [16]byte
	data := storage[:0]
	data = binary.AppendVarint(data, idm.ID)
	data = binary.AppendUvarint(data, uint64(len(idm.Pos)))
	buf.Write(data)
	for _, p := range idm.Pos {
		n := binary.PutUvarint(storage[:], p)
		buf.Write(storage[:n])
	}
}

func readVarint(data []byte) (int64, []byte, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("invalid varint (n=%d)", n)
	}
	return v, data[n:], nil
}

func readUvarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("invalid uvarint (n=%d)", n)
	}
	return v, data[n:], nil
}

func (idm *idMap) decodeBinary(data []byte) ([]byte, error) {
	uid, rest, err := readVarint(data)
	if err != nil {
		return nil, fmt.Errorf("idMap uid: %w", err)
	}
	l, rest, err := readUvarint(rest)
	if err != nil {
		return nil, fmt.Errorf("idMap length: %w", err)
	}
	if l > uint64(len(rest)) {
		return nil, fmt.Errorf("idMap truncated: need at least %d bytes, have %d", l, len(rest))
	}
	idm.ID = uid
	if l == 0 {
		idm.Pos = nil
		return rest, nil
	}
	idm.Pos = make([]uint64, int(l))
	for i := range idm.Pos {
		idm.Pos[i], rest, err = readUvarint(rest)
		if err != nil {
			return nil, fmt.Errorf("idMap pos[%d]: %w", i, err)
		}
	}
	return rest, nil
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
}

func (idms *idMaps) decodeBinary(data []byte) ([]byte, error) {
	l, rest, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("idMaps count: %w", err)
	}
	// Each encoded idMap requires at least 2 bytes (varint ID + uvarint length).
	if l > uint64(len(rest))/2 {
		return nil, fmt.Errorf("idMaps truncated: count %d exceeds remaining %d bytes", l, len(rest))
	}
	if l == 0 {
		*idms = nil
		return rest, nil
	}
	*idms = make([]idMap, int(l))
	for i := range *idms {
		rest, err = (*idms)[i].decodeBinary(rest)
		if err != nil {
			return nil, err
		}
	}
	return rest, nil
}

func (idms idMaps) idMapFor(id int64) int {
	for i, idm := range idms {
		if idm.ID == id {
			return i
		}
	}
	return -1
}

func (idms idMaps) idForPos(pos int) (int64, bool) {
	for _, idm := range idms {
		if idm.isSet(pos) {
			return idm.ID, true
		}
	}
	return 0, false
}

func newIDMap(id int64, n int) idMap {
	return idMap{
		ID:  id,
		Pos: make([]uint64, n/64+1),
	}
}

func (idm *idMap) set(pos int) {
	idm.Pos[pos/64] |= 1 << (pos % 64)
}

func (idm *idMap) isSet(pos int) bool {
	if pos < 0 || pos/64 >= len(idm.Pos) {
		return false
	}
	return idm.Pos[pos/64]>>(pos%64)&1 == 1
}

func newIDMapScanner(idm idMap) *idMapScanner {
	return &idMapScanner{
		idMap:  idm,
		bitPos: -1,
	}
}

type idMapScanner struct {
	idMap
	bitPos  int
	started bool
	done    bool
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
	if idm.done {
		return false
	}
	if !idm.started {
		idm.started = true
		idm.bitPos = select1(0, idm.Pos)
	} else {
		idm.bitPos++
		word := idm.bitPos / 64
		bit := idm.bitPos % 64
		idm.bitPos = select1(bit, idm.Pos[word:])
		if idm.bitPos >= 0 {
			idm.bitPos += word * 64
		}
	}
	if idm.bitPos < 0 {
		idm.done = true
		return false
	}
	return true
}

func (idm *idMapScanner) pos() int {
	return idm.bitPos
}
