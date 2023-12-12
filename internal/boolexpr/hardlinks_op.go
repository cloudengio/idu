// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr

import (
	"fmt"
	"reflect"
	"sync"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmdutil/boolexpr"
)

type Hardlink struct {
	text     string
	name     string
	document string
	value    bool
	requires reflect.Type
	mu       sync.Mutex
	detector *hardlinks.Incremental
}

type devInoIfc interface {
	DevIno() (uint64, uint64)
}

type nameIfc interface {
	Name() string
}

func (hl *Hardlink) Prepare() (boolexpr.Operand, error) {
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

func (hl *Hardlink) Eval(v any) bool {
	var dev, ino uint64
	switch t := v.(type) {
	case devInoIfc:
		dev, ino = t.DevIno()
	default:
		return false
	}
	hl.mu.Lock()
	defer hl.mu.Unlock()
	islink := hl.detector.Ref(dev, ino)
	if hl.value {
		return islink
	}
	return !islink
}

func (hl *Hardlink) String() string {
	if hl.value {
		return hl.name + "=true"
	}
	return hl.name + "=false"
}

func (hl *Hardlink) Document() string {
	return hl.document
}

func (hl *Hardlink) Needs(t reflect.Type) bool {
	return t.Implements(hl.requires)
}

// NewHardlink returns an operand that determines if the supplied value is,
// or is not, a hardlink to a previously seen file or directory. It is
// incremental and hence only detects the second and subsequent hardlink in
// a set of hardlinks, the first instance of the hardlink is not detected.
// It is intended to help avoid overcounting the resources shared by
// hardlinks.
func NewHardlink(n, v string) boolexpr.Operand {
	return &Hardlink{
		name:     n,
		text:     v,
		document: n + `=true|false matches if the directory or file is, or is not, a hard link to a previously seen directory or file. It is incremental and hence only detects the second and subsequent hardlink in a set of hardlinks, the first instance of the hardlink is not detected. It is intended to help avoid overcounting the resources shared by hardlinks.`,
		detector: &hardlinks.Incremental{},
	}
}
