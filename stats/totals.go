// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/binary"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

type Totals struct {
	ID           uint64
	Files        int64 // number of files
	Prefixes     int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
	PrefixBytes  int64 // total size of prefixes
}

type PerIDTotals []Totals

func (t *Totals) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, t.ID)
	data = binary.AppendVarint(data, t.Files)
	data = binary.AppendVarint(data, t.Bytes)
	data = binary.AppendVarint(data, t.StorageBytes)
	data = binary.AppendVarint(data, t.PrefixBytes)
	data = binary.AppendVarint(data, t.Prefixes)
	return data
}

func (t *Totals) MarshalBinary() (data []byte, err error) {
	return t.AppendBinary(make([]byte, 0, 100)), nil
}

func (t *Totals) DecodeBinary(data []byte) []byte {
	var n int
	id, n := binary.Uvarint(data)
	data = data[n:]
	t.ID = id
	t.Files, n = binary.Varint(data)
	data = data[n:]
	t.Bytes, n = binary.Varint(data)
	data = data[n:]
	t.StorageBytes, n = binary.Varint(data)
	data = data[n:]
	t.PrefixBytes, n = binary.Varint(data)
	data = data[n:]
	t.Prefixes, n = binary.Varint(data)
	return data[n:]
}

func (t *Totals) UnmarshalBinary(data []byte) error {
	t.DecodeBinary(data)
	return nil
}

func (pid PerIDTotals) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, uint64(len(pid)))
	for _, p := range pid {
		data = p.AppendBinary(data)
	}
	return data
}

func (pid PerIDTotals) MarshalBinary() (data []byte, err error) {
	return pid.AppendBinary(make([]byte, 0, 100)), nil
}

func (pid *PerIDTotals) DecodeBinary(data []byte) []byte {
	var n int
	l, n := binary.Uvarint(data)
	data = data[n:]
	*pid = make([]Totals, l)
	for i := range *pid {
		data = (*pid)[i].DecodeBinary(data)
	}
	return data
}

func (tl *PerIDTotals) UnmarshalBinary(data []byte) error {
	tl.DecodeBinary(data)
	return nil
}

func (t Totals) update(fi file.Info, hardlink bool, blocks int64, du diskusage.Calculator) Totals {
	if fi.IsDir() {
		t.Prefixes++
		if !hardlink {
			t.PrefixBytes += fi.Size()
		}
		return t
	}
	t.Files++
	if !hardlink {
		t.Bytes += fi.Size()
		t.StorageBytes += du.Calculate(fi.Size(), blocks)
	}
	return t
}

type perID map[uint64]Totals

func (pid perID) flatten() PerIDTotals {
	tl := make(PerIDTotals, 0, len(pid))
	for id, t := range pid {
		t.ID = id
		tl = append(tl, t)
	}
	if len(tl) == 0 {
		return nil
	}
	return tl
}

func ComputeTotals(prefix string, pi *prefixinfo.T, du diskusage.Calculator, match boolexpr.Matcher) (totals Totals, perUser, perGroup PerIDTotals) {
	user, group := make(perID), make(perID)
	for _, fi := range pi.InfoList() {
		if !match.Entry(prefix, pi, fi) {
			continue
		}
		hl := match.IsHardlink(prefix, pi, fi)
		xattr := pi.XAttrInfo(fi)
		totals = totals.update(fi, hl, xattr.Blocks, du)
		user[xattr.UID] = user[xattr.UID].update(fi, hl, xattr.Blocks, du)
		group[xattr.GID] = group[xattr.GID].update(fi, hl, xattr.Blocks, du)
	}
	return totals, user.flatten(), group.flatten()
}
