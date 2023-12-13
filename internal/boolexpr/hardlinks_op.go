// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr

import (
	"io/fs"
	"reflect"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
)

type Hardlink struct {
	text     string
	name     string
	document string
	fs       fs.FS
	dev, ino uint64
	requires reflect.Type
}

type devInoIfc interface {
	DevIno() (uint64, uint64)
}

func (hl *Hardlink) Prepare() (boolexpr.Operand, error) {
	f, err := hl.fs.Open(hl.text)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fi := file.NewInfoFromFileInfo(info)
	_, _, dev, ino, err := prefixinfo.GetSysInfo(hl.text, fi)
	if err != nil {
		return nil, err
	}
	hl.dev, hl.ino = dev, ino
	return hl, nil
}

func (hl Hardlink) Eval(v any) bool {
	var dev, ino uint64
	switch t := v.(type) {
	case devInoIfc:
		dev, ino = t.DevIno()
	default:
		return false
	}
	return dev == hl.dev && ino == hl.ino
}

func (hl *Hardlink) String() string {
	return hl.name + "=" + hl.text
}

func (hl *Hardlink) Document() string {
	return hl.document
}

func (hl *Hardlink) Needs(t reflect.Type) bool {
	return t.Implements(hl.requires)
}

// NewHardlink returns an operand that determines if the supplied value is,
// or is not, a hardlink to the specified file or directory.
func NewHardlink(n, v string, fs fs.FS) boolexpr.Operand {
	return &Hardlink{
		fs:       fs,
		name:     n,
		text:     v,
		document: n + `=<pathname>. Returns true if the evaluated value refers to the same file or directory as <pathname>, ie. if they share the same device and inode numbers.`,
	}
}
