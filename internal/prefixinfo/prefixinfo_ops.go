// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"fmt"
	"reflect"
	"strconv"

	"cloudeng.io/cmdutil/boolexpr"
)

type UserOrGroup struct {
	text     string
	id       uint32
	idLookup func(string) (uint32, error)
	document string
	group    bool
	requires reflect.Type
}

type userGroupIfc interface {
	UserGroup() (uint32, uint32)
}

func (op UserOrGroup) Prepare() (boolexpr.Operand, error) {
	op.requires = reflect.TypeOf((*userGroupIfc)(nil)).Elem()
	id, err := strconv.ParseUint(op.text, 10, 16)
	if err == nil || op.idLookup == nil {
		op.id = uint32(id)
		return op, nil
	}
	// Try to look up user/group name, rather than id.
	op.id, err = op.idLookup(op.text)
	if err != nil {
		if op.group {
			return op, fmt.Errorf("failed to lookup group: %q: %v", op.text, err)
		}
		return op, fmt.Errorf("failed to lookup user: %q: %v", op.text, err)
	}
	return op, nil

}

func (op UserOrGroup) Eval(v any) bool {
	var uid, gid uint32
	switch t := v.(type) {
	case userGroupIfc:
		uid, gid = t.UserGroup()
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

// IDLookup is a function that can be used to lookup a user or group id
// from a name.
type IDLookup func(name string) (uint32, error)

// NewUID returns an operand that matches the specified user id/name.
// The evaluated value must provide the method UserGroup() (uint32, uint32).
func NewUID(_, v string, idl IDLookup) boolexpr.Operand { return UserOrGroup{text: v} }

// NewGID returns an operand that matches the specified group id/name.
// The evaluated value must provide the method UserGroup() (uint32, uint32).
func NewGID(_, v string, idl IDLookup) boolexpr.Operand { return UserOrGroup{text: v, group: true} }

func RegisterOperands(p *boolexpr.Parser, uidLookup, gidLookup IDLookup) {
	p.RegisterOperand("user",
		func(_, v string) boolexpr.Operand {
			return UserOrGroup{
				text:     v,
				document: `uid=<uid/name> matches the specified user id/name`,
				idLookup: uidLookup}
		})
	p.RegisterOperand("group", func(_, v string) boolexpr.Operand {
		return UserOrGroup{
			text:     v,
			group:    true,
			document: `gid=<gid/name> matches the specified group id/name`,
			idLookup: gidLookup}
	})
}
