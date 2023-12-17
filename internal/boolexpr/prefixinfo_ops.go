// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr

import (
	"fmt"
	"reflect"
	"strconv"

	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file/filewalk"
)

type UserOrGroup struct {
	name     string
	text     string
	id       uint64
	idLookup func(string) (uint64, error)
	document string
	group    bool
	requires reflect.Type
}

type xattrIfc interface {
	XAttr() filewalk.XAttr
}

func (op UserOrGroup) Prepare() (boolexpr.Operand, error) {
	op.requires = reflect.TypeOf((*xattrIfc)(nil)).Elem()
	id, err := strconv.ParseUint(op.text, 10, 16)
	if err == nil || op.idLookup == nil {
		op.id = id
		return op, nil
	}
	// Try to look up user/group name, rather than id.
	nid, err := op.idLookup(op.text)
	if err != nil {
		if op.group {
			return op, fmt.Errorf("failed to lookup group: %q: %v", op.text, err)
		}
		return op, fmt.Errorf("failed to lookup user: %q: %v", op.text, err)
	}
	op.id = nid
	return op, nil
}

func (op UserOrGroup) Eval(v any) bool {
	var uid, gid uint64
	switch t := v.(type) {
	case xattrIfc:
		xattr := t.XAttr()
		uid, gid = xattr.UID, xattr.GID
	default:
		return false
	}
	if op.group {
		return gid == op.id
	}
	return uid == op.id
}

func (op UserOrGroup) String() string {
	if op.group {
		return "group=" + op.text
	}
	return "user=" + op.text
}

func (op UserOrGroup) Document() string {
	return op.document
}

func (op UserOrGroup) Needs(t reflect.Type) bool {
	return t.Implements(op.requires)
}

// NewUID returns an operand that matches the specified user id/name.
// The evaluated value must provide the method XAtrr() filewalk.XAttr.
func NewUID(n, v string, idl func(name string) (uint64, error)) boolexpr.Operand {
	return UserOrGroup{
		name:     n,
		text:     v,
		document: n + `=<uid/name> matches the specified user id/name`,
		idLookup: idl,
	}
}

// NewGID returns an operand that matches the specified group id/name.
// The evaluated value must provide the  method XAtrr() filewalk.XAttr.
func NewGID(n, v string, idl func(name string) (uint64, error)) boolexpr.Operand {
	return UserOrGroup{
		name:     n,
		text:     v,
		group:    true,
		document: n + `=<gid/name> matches the specified group id/name`,
		idLookup: idl,
	}
}
