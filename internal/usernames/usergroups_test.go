// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package usernames_test

import (
	"os/user"
	"strconv"
	"testing"

	"cloudeng.io/cmd/idu/internal/usernames"
)

func TestIDManager_CurrentUser(t *testing.T) {
	t.Parallel()

	cur, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}
	uid, err := strconv.ParseInt(cur.Uid, 10, 64)
	if err != nil {
		t.Skipf("non-numeric uid %q: %v", cur.Uid, err)
	}
	gid, err := strconv.ParseInt(cur.Gid, 10, 64)
	if err != nil {
		t.Skipf("non-numeric gid %q: %v", cur.Gid, err)
	}

	name := usernames.Manager.NameForUID(uid)
	if name == "" {
		t.Fatal("expected non-empty user name")
	}
	if gotUID, err := usernames.Manager.UIDForName(name); err != nil || gotUID != uid {
		t.Errorf("UIDForName(%q) = (%d, %v), want (%d, nil)", name, gotUID, err, uid)
	}

	gname := usernames.Manager.NameForGID(gid)
	if gname == "" {
		t.Fatal("expected non-empty group name")
	}
	if gotGID, err := usernames.Manager.GIDForName(gname); err != nil || gotGID != gid {
		t.Errorf("GIDForName(%q) = (%d, %v), want (%d, nil)", gname, gotGID, err, gid)
	}
}

func TestIDManager_NumericAndUnknown(t *testing.T) {
	t.Parallel()

	const missingID = int64(987654321)
	if got, want := usernames.Manager.NameForUID(missingID), "987654321"; got != want {
		t.Errorf("NameForUID(%d) = %q, want %q", missingID, got, want)
	}
	if got, want := usernames.Manager.NameForGID(missingID), "987654321"; got != want {
		t.Errorf("NameForGID(%d) = %q, want %q", missingID, got, want)
	}
	if got, err := usernames.Manager.UIDForName("12345"); err != nil || got != 12345 {
		t.Errorf("UIDForName(12345) = (%d, %v), want (12345, nil)", got, err)
	}
	if got, err := usernames.Manager.GIDForName("12345"); err != nil || got != 12345 {
		t.Errorf("GIDForName(12345) = (%d, %v), want (12345, nil)", got, err)
	}
	if _, err := usernames.Manager.UIDForName("idu-nonexistent-user-xyz"); err == nil {
		t.Error("expected error for non-existent user name")
	}
	if _, err := usernames.Manager.GIDForName("idu-nonexistent-group-xyz"); err == nil {
		t.Error("expected error for non-existent group name")
	}
}
