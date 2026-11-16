// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"encoding/binary"
	"fmt"
)

type Stats struct {
	ID           uint32
	Files        int64 // number of files
	Prefixes     int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
	PrefixBytes  int64 // total size of prefixes
}

type StatsList []Stats

func (s *Stats) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, uint64(s.ID))
	data = binary.AppendVarint(data, s.Files)
	data = binary.AppendVarint(data, s.Bytes)
	data = binary.AppendVarint(data, s.StorageBytes)
	data = binary.AppendVarint(data, s.PrefixBytes)
	data = binary.AppendVarint(data, s.Prefixes)
	return data
}

func (s *Stats) MarshalBinary() (data []byte, err error) {
	return s.AppendBinary(make([]byte, 0, 100)), nil
}

func (s *Stats) decodeBinary(data []byte) ([]byte, error) {
	id, rest, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("Stats.ID: %w", err)
	}
	s.ID = uint32(id) //nolint:gosec // ID fits in uint32 when encoded from Stats.ID
	fields := []*int64{&s.Files, &s.Bytes, &s.StorageBytes, &s.PrefixBytes, &s.Prefixes}
	for _, f := range fields {
		*f, rest, err = readVarint(rest)
		if err != nil {
			return nil, fmt.Errorf("Stats field: %w", err)
		}
	}
	return rest, nil
}

func (s *Stats) DecodeBinary(data []byte) []byte {
	rest, err := s.decodeBinary(data)
	if err != nil {
		return nil
	}
	return rest
}

func (s *Stats) UnmarshalBinary(data []byte) error {
	_, err := s.decodeBinary(data)
	return err
}

func (sl StatsList) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, uint64(len(sl)))
	for _, p := range sl {
		data = p.AppendBinary(data)
	}
	return data
}

func (sl StatsList) MarshalBinary() (data []byte, err error) {
	return sl.AppendBinary(make([]byte, 0, 100)), nil
}

func (sl *StatsList) decodeBinary(data []byte) ([]byte, error) {
	l, rest, err := readUvarint(data)
	if err != nil {
		return nil, fmt.Errorf("StatsList count: %w", err)
	}
	// Each encoded Stats requires at least 6 bytes (1 uvarint + 5 varints).
	if l > uint64(len(rest))/6 {
		return nil, fmt.Errorf("StatsList truncated: count %d exceeds remaining %d bytes", l, len(rest))
	}
	if l == 0 {
		*sl = nil
		return rest, nil
	}
	*sl = make([]Stats, int(l))
	for i := range *sl {
		rest, err = (*sl)[i].decodeBinary(rest)
		if err != nil {
			return nil, err
		}
	}
	return rest, nil
}

func (sl *StatsList) DecodeBinary(data []byte) []byte {
	rest, err := sl.decodeBinary(data)
	if err != nil {
		return nil
	}
	return rest
}

func (sl *StatsList) UnmarshalBinary(data []byte) error {
	_, err := sl.decodeBinary(data)
	return err
}
