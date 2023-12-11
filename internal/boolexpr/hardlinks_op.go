// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr

import (
	"fmt"
	"reflect"

	"cloudeng.io/cmdutil/boolexpr"
)

type perDeviceRefs struct {
	dev    uint64
	inodes map[uint64]struct{}
}

type Hardlink struct {
	text     string
	document string
	value    bool
	requires reflect.Type
	devices  []*perDeviceRefs
}

func (hl *Hardlink) forDevice(dev uint64) *perDeviceRefs {
	for _, d := range hl.devices {
		if d.dev == dev {
			return d
		}
	}
	hl.devices = append(hl.devices, &perDeviceRefs{
		dev:    dev,
		inodes: map[uint64]struct{}{},
	})
	return nil
}

type devInoIfc interface {
	DevIno() (uint64, uint64)
}

func (hl Hardlink) Prepare() (boolexpr.Operand, error) {
	switch hl.text {
	case "true":
		hl.value = true
	case "false":
		hl.value = false
	default:
		return hl, fmt.Errorf("invalid hardlink value: %q", hl.text)
	}
	hl.requires = reflect.TypeOf((*devInoIfc)(nil)).Elem()
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
	seen := hl.forDevice(dev)
	_, ok := seen.inodes[ino]
	if ok {
		return hl.value
	}
	seen.inodes[ino] = struct{}{}
	return !hl.value
}

func (hl Hardlink) String() string {
	if hl.value {
		return "hardlink=true"
	}
	return "hardlink=false"
}

func (hl Hardlink) Document() string {
	return hl.document
}

func (hl Hardlink) Needs(t reflect.Type) bool {
	return t.Implements(hl.requires)
}

// NewUID returns an operand that matches the specified user id/name.
// The evaluated value must provide the method UserGroup() (uint32, uint32).
func NewHardlink(_, v string) boolexpr.Operand {
	return UserOrGroup{
		text:     v,
		document: `hardlink=true|false matches if the directory or file is, or is not, a hard link`,
	}
}
