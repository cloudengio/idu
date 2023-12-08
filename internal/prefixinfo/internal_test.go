// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cloudeng.io/file"
)

type RoundTripper func(*testing.T, *T) T

func GobRoundTrip(t *testing.T, pi *T) T {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(pi); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	var nfi T
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&nfi); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	return nfi
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%v", filepath.Base(file), line)
}

func BinaryRoundTrip(t *testing.T, pi *T) T {
	buf, err := pi.MarshalBinary()
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	var npi T
	if err := npi.UnmarshalBinary(buf); err != nil {

		t.Fatalf("%s: %v", caller(), err)
	}
	return npi
}

// IDCombinations returns 5 sets of file.Info values with differing combinations
// of uid and gid.
// ug00 has uid, gid for both files
// ug10 has uid+1, gid for the second file
// ug01 has uid, gid+1 for the second file
// ug11 has uid+1, gid+1 for the second file
// ugOther has uid+1, gid+1 for both files
func TestdataIDCombinations(modTime time.Time, mode fs.FileMode, uid, gid uint32) (ug00, ug10, ug01, ug11, ugOther []file.Info) {
	ug00 = []file.Info{
		TestdataNewInfo("0", 1, mode, modTime, uid, gid),
		TestdataNewInfo("1", 2, mode, modTime, uid, gid),
	}
	ug10 = []file.Info{
		TestdataNewInfo("0", 1, mode, modTime, uid, gid),
		TestdataNewInfo("1", 2, mode, modTime, uid+1, gid),
	}
	ug01 = []file.Info{
		TestdataNewInfo("0", 1, mode, modTime, uid, gid),
		TestdataNewInfo("1", 2, mode, modTime, uid, gid+1),
	}
	ug11 = []file.Info{
		TestdataNewInfo("0", 1, mode, modTime, uid, gid),
		TestdataNewInfo("1", 2, mode, modTime, uid+1, gid+1),
	}
	ugOther = []file.Info{
		TestdataNewInfo("0", 1, mode, modTime, uid+1, gid+1),
		TestdataNewInfo("1", 2, mode, modTime, uid+1, gid+1),
	}
	return
}

func TestdataIDCombinationsFiles(modTime time.Time, uid, gid uint32) (ug00, ug10, ug01, ug11, ugOther []file.Info) {
	return TestdataIDCombinations(modTime, 0700, uid, gid)
}

func TestdataIDCombinationsDirs(modTime time.Time, uid, gid uint32) (ug00, ug10, ug01, ug11, ugOther []file.Info) {
	return TestdataIDCombinations(modTime, 0700|os.ModeDir, uid, gid)
}

func TestdataNewInfo(name string, size int64, mode fs.FileMode, modTime time.Time, uid, gid uint32) file.Info {
	return file.NewInfo(name, size, mode, modTime, SysUserGroupID(uid, gid))
}

func TestdataNewPrefixInfo(t *testing.T, name string, size int64, mode fs.FileMode, modTime time.Time, uid, gid uint32) T {
	fi := TestdataNewInfo(name, size, mode, modTime, uid, gid)
	pi, err := New(fi)
	if err != nil {
		t.Fatal(err)
	}
	return pi
}

func IDsFromStats(s StatsList) []uint32 {
	r := []uint32{}
	for _, st := range s {
		r = append(r, st.ID)
	}
	return r
}

func SetDevInode(pi *T, dev, ino uint64) {
	pi.device = dev
	pi.inodes = make([]uint64, len(pi.entries))
	for i := range pi.entries {
		pi.inodes[i] = ino + uint64(i)
	}
}

func GetDev(pi T) uint64 {
	return pi.device
}

func GetInodes(pi T) []uint64 {
	return pi.inodes
}
