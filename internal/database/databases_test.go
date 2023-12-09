// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package database_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/badgerdb"
	"github.com/dgraph-io/badger/v4"
	"golang.org/x/exp/slices"
)

func badgerFactory(t *testing.T, dir, prefix string, readonly bool) database.DB {
	t.Helper()
	dbname := filepath.Join(dir, "db")
	opts := []badgerdb.Option{}
	bopts := badger.DefaultOptions(dbname)
	bopts = bopts.WithLogger(nil)
	opts = append(opts, badgerdb.WithBadgerOptions(bopts))
	if readonly {
		opts = append(opts, badgerdb.ReadOnly())
	}
	db, err := badgerdb.Open(dbname, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type databaseFactory func(t *testing.T, dir, prefix string, readonly bool) database.DB

func populateDatabase(t *testing.T, db database.DB, nItems int) {
	ctx := context.Background()
	defer db.Close(ctx)
	for i := 0; i < nItems; i++ {
		if err := db.Set(ctx, fmt.Sprintf("/a/%02v", i), []byte(fmt.Sprintf("a%v", i)), false); err != nil {
			t.Fatal(err)
		}
	}
	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		for i := 0; i < nItems; i++ {
			if err := db.Set(ctx, fmt.Sprintf("/z/%02v", i), []byte(fmt.Sprintf("z%v", i)), true); err != nil {
				ch <- err
				return
			}
		}
		ch <- nil
	}()
	if err := <-ch; err != nil {
		t.Fatal(err)
	}
	db.LogError(ctx, "/a/01", time.Now(), []byte("error"))
	db.Log(ctx, time.Now(), time.Now(), []byte("log"))
	db.Close(ctx)
}

func validatePopulatedDatabase(t *testing.T, found []string, nItems int) {
	fmt.Printf("N ITEMS: %v\n", nItems)
	fmt.Printf("found: %v - %v\n", len(found), found)
	n, p := 0, "a"
	for i := 0; i < nItems*2; i++ {
		k, v := fmt.Sprintf("/%v/%02v", p, n), fmt.Sprintf("%v%v", p, n)
		if got, want := found[i], k+v; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		n++
		if i == nItems-1 {
			n, p = 0, "z"
		}
	}

}

func TestScan(t *testing.T) {
	testScan(t, badgerFactory)
}

func testScan(t *testing.T, factory databaseFactory) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	tmpdir := t.TempDir()
	db := factory(t, tmpdir, prefix, false)

	nItems := 100
	populateDatabase(t, db, nItems)

	db = factory(t, tmpdir, prefix, true)
	defer db.Close(ctx)

	found := []string{}
	err := db.Scan(ctx, "", func(_ context.Context, k string, v []byte) bool {
		found = append(found, k+string(v))
		return true
	})

	if err != nil {
		t.Fatal(err)
	}
	validatePopulatedDatabase(t, found, nItems)

	// Stream and Scan are identical for the entire database.
	found = []string{}
	err = db.Stream(ctx, "", func(_ context.Context, k string, v []byte) {
		found = append(found, k+string(v))
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	validatePopulatedDatabase(t, found, nItems)

	// Scan can be ised to implement a range scan.
	found = []string{}
	err = db.Scan(ctx, "/z/03", func(_ context.Context, k string, v []byte) bool {
		found = append(found, k+string(v))
		return strings.Compare(k, "/z/06") < 0
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(found), 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for i := 3; i < 6; i++ {
		k, v := fmt.Sprintf("/z/%02v", i), fmt.Sprintf("z%v", i)
		if got, want := found[i-3], k+v; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// Stream can be used to implement a concurrent prefix scan.
	found = []string{}
	err = db.Stream(ctx, "/z/0", func(_ context.Context, k string, v []byte) {
		found = append(found, k+string(v))
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(found), 10; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for i := 0; i < 9; i++ {
		k, v := fmt.Sprintf("/z/%02v", i), fmt.Sprintf("z%v", i)
		if got, want := found[i], k+v; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

}

func TestLogAndClose(t *testing.T) {
	testLogAndClose(t, badgerFactory)
}

func testLogAndClose(t *testing.T, factory databaseFactory) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	tmpdir := t.TempDir()
	t1, _ := time.Parse(time.RFC3339, "2023-08-10T10:00:02-08:00")
	t2, _ := time.Parse(time.RFC3339, "2023-08-11T10:00:02-08:00")
	t3, _ := time.Parse(time.RFC3339, "2023-08-12T10:00:02-08:00")
	times := []time.Time{t1, t2, t3}
	for i, start := range times {
		db := factory(t, tmpdir, prefix, false)
		op := fmt.Sprintf("%v", i)
		if err := db.Log(ctx, start, start.Add(time.Hour), []byte(op)); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}

	db := factory(t, tmpdir, prefix, true)
	defer db.Close(ctx)

	ls, le, pl, err := db.LastLog(ctx)
	if err != nil {
		t.Fatal(err)
	}

	match := func(i int, start, stop time.Time, detail []byte) {
		if got, want := start, times[i]; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := stop, times[i].Add(time.Hour); !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := detail, []byte(fmt.Sprintf("%v", i)); !bytes.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	match(len(times)-1, ls, le, pl)

	entries := 0
	err = db.VisitLogs(ctx, time.Time{}, time.Now(),
		func(_ context.Context, begin, end time.Time, detail []byte) bool {
			match(entries, begin, end, detail)
			entries++
			return true
		})

	if got, want := entries, 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	testErrors(t, badgerFactory)
}

func testErrors(t *testing.T, factory databaseFactory) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	tmpdir := t.TempDir()
	db := factory(t, tmpdir, prefix, false)
	defer db.Close(ctx)

	t1, _ := time.Parse(time.RFC3339, "2023-08-10T10:00:02-08:00")
	t2, _ := time.Parse(time.RFC3339, "2023-08-11T10:00:02-08:00")
	t3, _ := time.Parse(time.RFC3339, "2023-08-12T10:00:02-08:00")
	times := []time.Time{t1, t2, t3}
	for i, when := range times {
		key := fmt.Sprintf("/%02v", i)
		op := fmt.Sprintf("%02v", i)
		if err := db.LogError(ctx, key, when, []byte(op)); err != nil {
			t.Fatal(err)
		}
	}

	match := func(i int, key string, when time.Time, detail []byte) {
		if got, want := when, times[i]; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		op := fmt.Sprintf("%02v", i)
		if got, want := detail, []byte(op); !bytes.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	entries := 1
	err := db.VisitErrors(ctx, "/01",
		func(_ context.Context, key string, when time.Time, detail []byte) bool {
			match(entries, key, when, detail)
			entries++
			return true
		})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := entries, 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func visitAllErrors(t *testing.T, ctx context.Context, db database.DB) []string {
	keys := map[string]struct{}{}
	err := db.VisitErrors(ctx, "",
		func(_ context.Context, key string, when time.Time, detail []byte) bool {
			keys[key] = struct{}{}
			return true
		})
	if err != nil {
		t.Fatal(err)
	}
	k := []string{}
	for key := range keys {
		k = append(k, key)
	}
	sort.Strings(k)
	return k
}

func TestErrorsDelete(t *testing.T) {
	testErrorsDelete(t, badgerFactory)
}

func testErrorsDelete(t *testing.T, factory databaseFactory) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	tmpdir := t.TempDir()
	db := factory(t, tmpdir, prefix, false)
	defer db.Close(ctx)

	now := time.Now()
	for i := 0; i < 100; i++ {
		op := fmt.Sprintf("%02v", i)
		prefix := fmt.Sprintf("/%v/%v", i/10, i%10)
		if err := db.LogError(ctx, prefix, now, []byte(op)); err != nil {
			t.Fatal(err)
		}
	}

	keys := visitAllErrors(t, ctx, db)
	if got, want := len(keys), 100; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if err := db.DeleteErrors(ctx, "/1/"); err != nil {
		t.Fatal(err)
	}

	keys = visitAllErrors(t, ctx, db)
	if got, want := len(keys), 90; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	n := 0
	for _, p := range []int{0, 2, 3, 4, 5, 6, 7, 8, 9} {
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("/%v/%v", p, j)
			if got, want := keys[n], k; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			n++
		}
	}
}

func TestDelete(t *testing.T) {
	testDelete(t, badgerFactory)
}

func testDelete(t *testing.T, factory databaseFactory) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	tmpdir := t.TempDir()
	db := factory(t, tmpdir, prefix, false)
	defer db.Close(ctx)

	keys := []string{}
	nItems := 50
	for i := 0; i < nItems; i++ {
		keys = append(keys, fmt.Sprintf("/%03v", i))
	}
	for _, k := range keys {
		if err := db.Set(ctx, k, []byte(k), false); err != nil {
			t.Fatal(err)
		}
	}

	scan := func() []string {
		keys := []string{}
		err := db.Scan(ctx, "", func(_ context.Context, k string, v []byte) bool {
			keys = append(keys, k)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		return keys
	}

	left := slices.Delete(keys, 10, 20)
	if err := db.DeletePrefix(ctx, "/01"); err != nil {
		t.Fatal(err)
	}

	if got, want := scan(), left; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if err := db.DeletePrefix(ctx, "notthere"); err != nil {
		t.Fatal(err)
	}

}

func TestExists(t *testing.T) {
	testExists(t, badgerFactory)
}

func testExists(t *testing.T, factory databaseFactory) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	tmpdir := t.TempDir()
	db := factory(t, tmpdir, prefix, false)
	defer db.Close(ctx)
	var buf bytes.Buffer
	err := db.Get(ctx, "/a/b/c", &buf)
	if err != nil {
		t.Fatal(err)
	}
	if k := buf.Bytes(); k != nil {
		t.Errorf("got %v, want nil", k)
	}
}
