// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/binary"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

type Totals struct {
	ID           uint32
	Files        int64 // number of files
	Prefixes     int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
	PrefixBytes  int64 // total size of prefixes
}

type PerIDTotals []Totals

func (t *Totals) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, uint64(t.ID))
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
	t.ID = uint32(id)
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

type Matcher interface {
	Prefix(prefix string, pi *prefixinfo.T) bool
	Entry(prefix string, pi *prefixinfo.T, fi file.Info) bool
}

func (t Totals) Update(fi file.Info, du diskusage.Calculator) Totals {
	if fi.IsDir() {
		t.Prefixes++
		t.PrefixBytes += fi.Size()
		return t
	}
	t.Files++
	t.Bytes += fi.Size()
	t.StorageBytes += du.Calculate(fi.Size())
	return t
}

type perID map[uint32]Totals

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

func ComputeTotals(prefix string, pi *prefixinfo.T, du diskusage.Calculator, match Matcher) (totals Totals, perUser, perGroup PerIDTotals) {
	if !match.Prefix(prefix, pi) {
		return
	}
	user, group := make(perID), make(perID)
	for _, fi := range pi.InfoList() {
		uid, gid, _, _ := pi.SysInfo(fi)
		if !match.Entry(prefix, pi, fi) {
			continue
		}
		totals = totals.Update(fi, du)
		user[uid] = user[uid].Update(fi, du)
		group[gid] = group[gid].Update(fi, du)
	}
	return totals, user.flatten(), group.flatten()
}