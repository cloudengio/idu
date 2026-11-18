// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"io/fs"
	"time"

	"cloudeng.io/file"
)

// PrefixInfo represents the metadata recorded for a single prefix
// (directory) that is needed to compute its statistics and to evaluate
// expressions against it and its entries. It is implemented by the
// representation used for prefixes in the database. The MatchPrefix values
// that expressions are evaluated against embed a PrefixInfo, and MatchEntry
// values provide access to the PrefixInfo for the entry's prefix, so that
// custom operands, see WithOperand, can use any of its methods.
type PrefixInfo interface {
	// Size returns the size of the prefix itself.
	Size() int64
	// Blocks returns the number of blocks allocated to the prefix itself.
	Blocks() int64
	// Mode returns the mode of the prefix.
	Mode() fs.FileMode
	// Type returns the type bits of the prefix's mode.
	Type() fs.FileMode
	// ModTime returns the modification time of the prefix.
	ModTime() time.Time
	// UserGroup returns the user and group IDs of the prefix.
	UserGroup() (uid, gid int64)
	// DevIno returns the device and inode numbers of the prefix.
	DevIno() (device, inode uint64)
	// XAttr returns the user, group, device, inode etc. of the prefix.
	XAttr() file.XAttr
	// InfoList returns the prefix's entries, ie. its files and its
	// immediate child prefixes, for which IsDir returns true.
	InfoList() file.InfoList
	// FilesOnly returns the entries that are files, ie. not prefixes.
	FilesOnly() file.InfoList
	// PrefixesOnly returns the entries that are prefixes.
	PrefixesOnly() file.InfoList
	// XAttrInfo returns the user, group, device, inode etc. of fi, which
	// must be one of the entries returned by InfoList, FilesOnly or
	// PrefixesOnly.
	XAttrInfo(fi file.Info) file.XAttr
}
