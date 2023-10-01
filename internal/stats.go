// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import "encoding/binary"

type Stats struct {
	ID           uint32
	Files        uint64 // number of files
	Dirs         uint64 // number of directories
	Bytes        uint64 // total size of files
	StorageBytes uint64 // total size of files on disk
}

type StatsList []Stats

func (s *Stats) AppendBinary(data []byte) ([]byte, error) {
	data = binary.AppendUvarint(data, uint64(s.ID))
	data = binary.AppendUvarint(data, uint64(s.Files))
	data = binary.AppendUvarint(data, uint64(s.Dirs))
	data = binary.AppendUvarint(data, uint64(s.Bytes))
	data = binary.AppendUvarint(data, uint64(s.StorageBytes))
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
	s.Files, n = binary.Uvarint(data)
	data = data[n:]
	s.Dirs, n = binary.Uvarint(data)
	data = data[n:]
	s.Bytes, n = binary.Uvarint(data)
	data = data[n:]
	s.StorageBytes, n = binary.Uvarint(data)
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
