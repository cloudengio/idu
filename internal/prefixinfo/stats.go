// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import "encoding/binary"

type Stats struct {
	ID           uint32
	Files        int64 // number of files
	Prefixes     int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
}

type StatsList []Stats

func (s *Stats) AppendBinary(data []byte) ([]byte, error) {
	data = binary.AppendUvarint(data, uint64(s.ID))
	data = binary.AppendVarint(data, s.Files)
	data = binary.AppendVarint(data, s.Bytes)
	data = binary.AppendVarint(data, s.StorageBytes)
	return data, nil
}

func (s *Stats) MarshalBinary() (data []byte, err error) {
	return s.AppendBinary(make([]byte, 0, 100))
}

func (s *Stats) DecodeBinary(data []byte) ([]byte, error) {
	var n int
	id, n := binary.Uvarint(data)
	data = data[n:]
	s.ID = uint32(id)
	s.Files, n = binary.Varint(data)
	data = data[n:]
	s.Bytes, n = binary.Varint(data)
	data = data[n:]
	s.StorageBytes, n = binary.Varint(data)
	return data[n:], nil
}

func (s *Stats) UnmarshalBinary(data []byte) error {
	_, err := s.DecodeBinary(data)
	return err
}

func (sl StatsList) AppendBinary(data []byte) ([]byte, error) {
	data = binary.AppendUvarint(data, uint64(len(sl)))
	for _, p := range sl {
		data, _ = p.AppendBinary(data)
	}
	return data, nil
}

func (sl StatsList) MarshalBinary() (data []byte, err error) {
	return sl.AppendBinary(make([]byte, 0, 100))
}

func (sl *StatsList) DecodeBinary(data []byte) ([]byte, error) {
	var n int
	l, n := binary.Uvarint(data)
	data = data[n:]
	*sl = make([]Stats, l)
	for i := range *sl {
		data, _ = (*sl)[i].DecodeBinary(data)
	}
	return data, nil
}

func (sl *StatsList) UnmarshalBinary(data []byte) error {
	_, err := sl.DecodeBinary(data)
	return err
}
