// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb_test

/*
func createUsrGroupEntries(t *testing.T, ctx context.Context, db database.DB) {
	if err := db.SetUser(ctx, 50, "u1", []uint32{32, 12}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetUser(ctx, 51, "u2", []uint32{32, 12, 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetGroup(ctx, 1, "g1"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetGroup(ctx, 12, "g2"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetGroup(ctx, 31, "g3"); err != nil {
		t.Fatal(err)
	}
}

func testUsers(t *testing.T, ctx context.Context, db database.DB) {
	err := db.VisitUsers(ctx, "", func(_ context.Context, uid uint32, u string, gids []uint32) bool {
		switch uid {
		case 50:
			if got, want := u, "u1"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := []uint32{32, 12}, gids; !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		case 51:
			if got, want := u, "u2"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := []uint32{32, 12, 1}, gids; !slices.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testGroups(t *testing.T, ctx context.Context, db database.DB) {
	err := db.VisitGroups(ctx, "", func(_ context.Context, gid uint32, g string) bool {
		switch gid {
		case 1:
			if got, want := g, "g1"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		case 12:
			if got, want := g, "g2"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		case 31:
			if got, want := g, "g3"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUserGroups(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesystem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

	createUsrGroupEntries(t, ctx, db)

	testUsers(t, ctx, db)
	testGroups(t, ctx, db)

}
*/
