// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb_test

import (
	"context"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/internal/database/boltdb"
	"golang.org/x/exp/slices"
)

func TestUserGroups(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesystem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.SetUser(ctx, "u1", 50, []int64{32, 12}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetUser(ctx, "u2", 51, []int64{32, 12, 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetGroup(ctx, "g1", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.SetGroup(ctx, "g2", 12); err != nil {
		t.Fatal(err)
	}
	if err := db.SetGroup(ctx, "g3", 31); err != nil {
		t.Fatal(err)
	}

	err = db.VisitUsers(ctx, "", func(_ context.Context, u string, uid int64, gids []int64) bool {
		switch u {
		case "u1":
			if got, want := uid, int64(50); got != want {
				t.Errorf("got %v, want 50", uid)
			}
			if got, want := []int64{32, 12}, gids; !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		case "u2":
			if got, want := uid, int64(51); got != want {
				t.Errorf("got %v, want 51", uid)
			}
			if got, want := []int64{32, 12, 1}, gids; !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}

		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.VisitGroups(ctx, "", func(_ context.Context, g string, gid int64) bool {
		switch g {
		case "g1":
			if got, want := gid, int64(1); got != want {
				t.Errorf("got %v, want 1", gid)
			}
		case "g2":
			if got, want := gid, int64(12); got != want {
				t.Errorf("got %v, want 12", gid)
			}
		case "g3":
			if got, want := gid, int64(31); got != want {
				t.Errorf("got %v, want 31", gid)
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}
