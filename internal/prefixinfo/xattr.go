// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"fmt"

	"cloudeng.io/file"
)

type fsOnly struct {
	ino    uint64
	blocks int64
}

type idAndFS struct {
	fsOnly
	uid, gid int64
}

func (pi *T) xAttrFromSys(v any) file.XAttr {
	switch s := v.(type) {
	case fsOnly:
		return file.XAttr{
			UID:    pi.xattr.UID,
			GID:    pi.xattr.GID,
			Device: pi.xattr.Device,
			FileID: s.ino,
			Blocks: s.blocks}
	case idAndFS:
		return file.XAttr{
			UID:    s.uid,
			GID:    s.gid,
			Device: pi.xattr.Device,
			FileID: s.ino,
			Blocks: s.blocks}
	case *file.XAttr:
		return *s
	case file.XAttr:
		return s
	}
	panic(fmt.Sprintf("unrecognised system information %T", v))
}

// NewSysInfo is intended to be used by tests.
func NewSysInfo(uid, gid int64, dev, ino uint64, blocks int64) any {
	return &file.XAttr{
		UID:    uid,
		GID:    gid,
		Device: dev,
		FileID: ino,
		Blocks: blocks}
}
