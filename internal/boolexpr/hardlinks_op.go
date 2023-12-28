// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr

import (
	"context"
	"reflect"

	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/matcher"
)

type Hardlink struct {
	ctx      context.Context
	text     string
	name     string
	document string
	fs       filewalk.FS
	dev, ino uint64
	requires reflect.Type
}

func prepare(ctx context.Context, name string, fs file.FS) (file.XAttr, error) {
	f, err := fs.Open(name)
	if err != nil {
		return file.XAttr{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return file.XAttr{}, err
	}
	fi := file.NewInfoFromFileInfo(info)
	xattr, err := fs.XAttr(ctx, name, fi)
	if err != nil {
		return file.XAttr{}, err
	}
	return file.XAttr{
		Device: xattr.Device,
		FileID: xattr.FileID,
	}, nil
}

// NewHardlink returns an operand that determines if the supplied value is,
// or is not, a hardlink to the specified file or directory.
func NewHardlink(ctx context.Context, n, v string, fs file.FS) boolexpr.Operand {
	return matcher.XAttr(
		n, v, `=<pathname>. Returns true if the evaluated value refers to the same file or directory as <pathname>, ie. if they share the same device and inode numbers.`,
		func(text string) (file.XAttr, error) {
			return prepare(ctx, text, fs)
		},
		func(opVal, val file.XAttr) bool {
			return opVal.Device == val.Device && opVal.FileID == val.FileID
		},
	)
}
