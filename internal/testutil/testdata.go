// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"io/fs"
	"os"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
)

// IDCombinations returns 5 sets of file.Info values with differing combinations
// of uid and gid.
// ug00 has uid, gid for both files
// ug10 has uid+1, gid for the second file
// ug01 has uid, gid+1 for the second file
// ug11 has uid+1, gid+1 for the second file
// ugOther has uid+1, gid+1 for both files
func TestdataIDCombinations(modTime time.Time, mode fs.FileMode, uid, gid int64, inode uint64) (ug00, ug10, ug01, ug11, ugOther []file.Info) {
	ug00 = []file.Info{
		TestdataNewInfo("0", 1, 1, mode, modTime, uid, gid, 0, inode),
		TestdataNewInfo("1", 2, 2, mode, modTime, uid, gid, 0, inode),
	}
	ug10 = []file.Info{
		TestdataNewInfo("0", 1, 1, mode, modTime, uid, gid, 0, inode),
		TestdataNewInfo("1", 2, 2, mode, modTime, uid+1, gid, 0, inode),
	}
	ug01 = []file.Info{
		TestdataNewInfo("0", 1, 1, mode, modTime, uid, gid, 0, inode),
		TestdataNewInfo("1", 2, 2, mode, modTime, uid, gid+1, 0, inode),
	}
	ug11 = []file.Info{
		TestdataNewInfo("0", 1, 1, mode, modTime, uid, gid, 0, inode),
		TestdataNewInfo("1", 2, 2, mode, modTime, uid+1, gid+1, 0, inode),
	}
	ugOther = []file.Info{
		TestdataNewInfo("0", 1, 1, mode, modTime, uid+1, gid+1, 0, inode),
		TestdataNewInfo("1", 2, 2, mode, modTime, uid+1, gid+1, 0, inode),
	}
	return
}

func TestdataIDCombinationsFiles(modTime time.Time, uid, gid int64, inode uint64) (ug00, ug10, ug01, ug11, ugOther []file.Info) {
	return TestdataIDCombinations(modTime, 0700, uid, gid, inode)
}

func TestdataIDCombinationsDirs(modTime time.Time, uid, gid int64, inode uint64) (ug00, ug10, ug01, ug11, ugOther []file.Info) {
	return TestdataIDCombinations(modTime, 0700|os.ModeDir, uid, gid, inode)
}

func TestdataNewInfo(name string, size, blocks int64, mode fs.FileMode, modTime time.Time, uid, gid int64, device, inode uint64) file.Info {
	return file.NewInfo(name, size, mode, modTime,
		prefixinfo.NewSysInfo(uid, gid, device, inode, blocks))
}

func TestdataNewPrefixInfo(name string, size, blocks int64, mode fs.FileMode, modTime time.Time, uid, gid int64, dev, inode uint64) prefixinfo.T {
	pi := prefixinfo.New(name, TestdataNewInfo(name, size, blocks, mode, modTime, uid, gid, dev, inode))
	return pi
}
