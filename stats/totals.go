// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"encoding/binary"
	"fmt"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file/diskusage"
)

// totalsVersion is the version of the binary encoding of Totals.
// TODO: add a new version for windows since IDs will be strings there.
const totalsVersion = 0x2

// TODO: parametize the ID type to be int64 or string for non-posix system.
type Totals struct {
	ID           int64
	Files        int64 // number of files
	Prefix       int64 // will be 1 or 0
	SubPrefixes  int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
	PrefixBytes  int64 // total size of prefixes
	Hardlinks    int64 // number of hardlinks
	HardlinkDirs int64 // number of hardlinks to directories
	SparseFiles  int64 // number of sparse files, see SparseBytes
	SparseBytes  int64 // total size of sparse files not allocated on disk
}

// SparseThreshold is the minimum number of bytes of a file's size that
// must not be allocated on disk for it to be considered sparse. It
// excludes small files whose data is stored inline in their inode or
// directory entry and hence have no allocated blocks, as well as
// platforms, such as Windows, that derive the number of blocks from the
// file's size.
const SparseThreshold = 64 * 1024

// blockSize is the unit of file.XAttr.Blocks, i.e. of st_blocks.
const blockSize = 512

// SparseBytes returns the number of bytes of a file of size bytes that
// occupies blocks blocks that are not allocated on disk, or zero if
// that is less than SparseThreshold. Note that compressed files will
// also appear sparse.
func SparseBytes(size, blocks int64) int64 {
	if unallocated := size - blocks*blockSize; unallocated >= SparseThreshold {
		return unallocated
	}
	return 0
}

type PerIDTotals []Totals

func (t *Totals) AppendBinary(data []byte) []byte {
	data = binary.AppendVarint(data, totalsVersion)
	data = binary.AppendVarint(data, t.ID)
	data = binary.AppendVarint(data, t.Files)
	data = binary.AppendVarint(data, t.Bytes)
	data = binary.AppendVarint(data, t.StorageBytes)
	data = binary.AppendVarint(data, t.PrefixBytes)
	data = binary.AppendVarint(data, t.Prefix)
	data = binary.AppendVarint(data, t.SubPrefixes)
	data = binary.AppendVarint(data, t.Hardlinks)
	data = binary.AppendVarint(data, t.HardlinkDirs)
	data = binary.AppendVarint(data, t.SparseFiles)
	data = binary.AppendVarint(data, t.SparseBytes)
	return data
}

func (t *Totals) MarshalBinary() (data []byte, err error) {
	return t.AppendBinary(make([]byte, 0, 100)), nil
}

func (t *Totals) DecodeBinary(data []byte) []byte {
	var n int
	ver, n := binary.Varint(data)
	if ver != totalsVersion {
		panic(fmt.Sprintf("unsupported version: %v", ver))
	}
	data = data[n:]
	id, n := binary.Varint(data)
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
	t.Prefix, n = binary.Varint(data)
	data = data[n:]
	t.SubPrefixes, n = binary.Varint(data)
	data = data[n:]
	t.Hardlinks, n = binary.Varint(data)
	data = data[n:]
	t.HardlinkDirs, n = binary.Varint(data)
	data = data[n:]
	t.SparseFiles, n = binary.Varint(data)
	data = data[n:]
	t.SparseBytes, n = binary.Varint(data)
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

func (pid *PerIDTotals) UnmarshalBinary(data []byte) error {
	pid.DecodeBinary(data)
	return nil
}

func (t Totals) update(bytes, storageBytes, sparseBytes int64) Totals {
	t.Files++
	t.Bytes += bytes
	t.StorageBytes += storageBytes
	if sparseBytes > 0 {
		t.SparseFiles++
		t.SparseBytes += sparseBytes
	}
	return t
}

func (t Totals) incHardlinks() Totals {
	t.Hardlinks++
	return t
}

func (t Totals) incSubPrefixes() Totals {
	t.SubPrefixes++
	return t
}

// Add accumulates the counts and byte totals from other into t.
func (t *Totals) Add(other Totals) {
	t.Files += other.Files
	t.Prefix += other.Prefix
	t.SubPrefixes += other.SubPrefixes
	t.Bytes += other.Bytes
	t.StorageBytes += other.StorageBytes
	t.PrefixBytes += other.PrefixBytes
	t.Hardlinks += other.Hardlinks
	t.HardlinkDirs += other.HardlinkDirs
	t.SparseFiles += other.SparseFiles
	t.SparseBytes += other.SparseBytes
}

type perID map[int64]Totals

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

const verbose = false

// ComputeTotals computes the totals for the prefix itself and any non-directory
// contents. Hardlinks are handled as per match.IsHardlink. Note that:
//  1. Prefixes is one if the prefix matched the expression and zero otherwise.
//  2. SubPrefixes is the number of prefixes this prefix contains
//  3. The size of this prefix is included in the totals for the prefix, but
//     the sizes of prefixes it contains are not.
func ComputeTotals(prefix string, pi *prefixinfo.T, du diskusage.Calculator, match Matcher) (totals Totals, perUser, perGroup PerIDTotals) {
	if !match.Prefix(prefix, pi) {
		return
	}
	totals.Prefix = 1
	xattr := pi.XAttr()
	if match.IsHardlink(xattr) {
		totals.HardlinkDirs = 1
		return
	}
	totals.PrefixBytes = pi.Size()
	totals.Bytes = pi.Size()
	totals.StorageBytes = du.Calculate(pi.Size(), xattr.Blocks)

	user, group := make(perID), make(perID)
	user[xattr.UID] = totals
	group[xattr.GID] = totals

	var blocks int64
	if verbose {
		blocks = xattr.Blocks
	}

	for _, fi := range pi.InfoList() {
		if !match.Entry(prefix, pi, fi) {
			continue
		}
		if fi.IsDir() {
			totals.SubPrefixes++
			user[xattr.UID] = user[xattr.UID].incSubPrefixes()
			group[xattr.GID] = group[xattr.GID].incSubPrefixes()
			continue
		}
		xattr := pi.XAttrInfo(fi)
		if match.IsHardlink(xattr) {
			totals.Hardlinks++
			user[xattr.UID] = user[xattr.UID].incHardlinks()
			group[xattr.GID] = group[xattr.GID].incHardlinks()
			continue
		}

		bytes := fi.Size()
		storageBytes := du.Calculate(bytes, xattr.Blocks)
		sparseBytes := SparseBytes(bytes, xattr.Blocks)
		totals = totals.update(bytes, storageBytes, sparseBytes)
		user[xattr.UID] = user[xattr.UID].update(bytes, storageBytes, sparseBytes)
		group[xattr.GID] = group[xattr.GID].update(bytes, storageBytes, sparseBytes)

		if verbose {
			fmt.Printf("%v\t%v/%v\n", (xattr.Blocks*512)/1024, prefix, fi.Name())
			blocks += xattr.Blocks
		}
	}

	if verbose {
		kb := (blocks * 512) / 1024
		fmt.Printf("%v\t%v/\n", kb, prefix)
	}

	return totals, user.flatten(), group.flatten()
}
