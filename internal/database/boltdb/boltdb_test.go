// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
)

func listBuckets(db database.DB) ([]string, error) {
	bdb := db.(*boltdb.Database).Bolt()
	buckets := []string{}
	err := bdb.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(tl []byte, b *bolt.Bucket) error {
			buckets = append(buckets, string(tl))
			err := b.ForEachBucket(func(nb []byte) error {
				buckets = append(buckets, string(tl)+"/"+string(nb))
				return nil
			})
			return err
		})
	})
	return buckets, err
}

func bucketsForPrefix(prefix string) []string {
	b := []string{prefix}
	for _, nb := range boltdb.NestedBuckets() {
		b = append(b, prefix+"/"+nb)
	}
	sort.Strings(b)
	return b
}

func TestOpen(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesystem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

	// Will timeout as the database is locked.
	_, err = boltdb.Open(dbname, prefix, boltdb.WithTimeout(10*time.Millisecond))
	if err == nil || err.Error() != "timeout" {
		t.Fatal(err)
	}

	db.Close(ctx)

	db, err = boltdb.Open(dbname, prefix, boltdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}

	buckets, err := listBuckets(db)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := buckets, bucketsForPrefix(prefix); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// It's possible to open multiple databases in read-only mode.
	db2, err := boltdb.Open(dbname, prefix, boltdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}

	buckets, err = listBuckets(db2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := buckets, bucketsForPrefix(prefix); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

}

func TestLogAndClose(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")

	t1, _ := time.Parse(time.RFC3339, "2023-08-10T10:00:02-08:00")
	t2, _ := time.Parse(time.RFC3339, "2023-08-11T10:00:02-08:00")
	t3, _ := time.Parse(time.RFC3339, "2023-08-12T10:00:02-08:00")
	times := []time.Time{t1, t2, t3}
	for i, start := range times {
		db, err := boltdb.Open(dbname, prefix)
		if err != nil {
			t.Fatal(err)
		}
		op := fmt.Sprintf("%v", i)
		if err := db.LogAndClose(ctx, start, start.Add(time.Hour), []byte(op)); err != nil {
			t.Fatal(err)
		}
	}

	db, err := boltdb.Open(dbname, prefix, boltdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
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

func TestScan(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")

	db, err := boltdb.Open(dbname, prefix, boltdb.BatchDelay(0))
	if err != nil {
		t.Fatal(err)
	}

	nItems := 100
	for i := 0; i < nItems; i++ {
		if err := db.Set(ctx, fmt.Sprintf("/a/%02v", i), []byte(fmt.Sprintf("a%v", i))); err != nil {
			t.Fatal(err)
		}
	}
	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		for i := 0; i < nItems; i++ {
			if err := db.SetBatch(ctx, fmt.Sprintf("/z/%02v", i), []byte(fmt.Sprintf("z%v", i))); err != nil {
				ch <- err
				return
			}
		}
		ch <- nil
	}()
	if err := <-ch; err != nil {
		t.Fatal(err)
	}
	db.Close(ctx)

	db, err = boltdb.Open(dbname, prefix, boltdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	n, p := 0, "a"
	err = db.Scan(ctx, "", func(_ context.Context, k string, v []byte) bool {
		if got, want := k, fmt.Sprintf("/%v/%02v", p, n); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(v), fmt.Sprintf("%v%v", p, n); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		n++
		if n == nItems {
			n, p = 0, "z"
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	n, p = 3, "z"
	err = db.Scan(ctx, "/z/03", func(_ context.Context, k string, v []byte) bool {
		if got, want := k, fmt.Sprintf("/%v/%02v", p, n); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(v), fmt.Sprintf("%v%v", p, n); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		n++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

	t1, _ := time.Parse(time.RFC3339, "2023-08-10T10:00:02-08:00")
	t2, _ := time.Parse(time.RFC3339, "2023-08-11T10:00:02-08:00")
	t3, _ := time.Parse(time.RFC3339, "2023-08-12T10:00:02-08:00")
	times := []time.Time{t1, t2, t3}
	for i, when := range times {
		key := fmt.Sprintf("/%02v", i)
		op := fmt.Sprintf("%02v", i)
		fmt.Printf("ADD: %v - %v\n", key, op)
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
	err = db.VisitErrors(ctx, "/01",
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
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

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
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{}
	nItems := 50
	for i := 0; i < nItems; i++ {
		keys = append(keys, fmt.Sprintf("/%03v", i))
	}
	for _, k := range keys {
		if err := db.Set(ctx, k, []byte(k)); err != nil {
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

	left := []string{}
	rmIdx := []int{27, 38, 41}
	rmKeys := []string{}
	for _, i := range rmIdx {
		rmKeys = append(rmKeys, fmt.Sprintf("/%03v", i))
	}

	for i := 0; i < nItems; i++ {
		if !slices.Contains(rmIdx, i) {
			left = append(left, keys[i])
		}
	}

	if err := db.Delete(ctx, rmKeys...); err != nil {
		t.Fatal(err)
	}

	if got, want := scan(), left; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	left = slices.Delete(left, 10, 20)
	if err := db.DeletePrefix(ctx, "/01"); err != nil {
		t.Fatal(err)
	}

	if got, want := scan(), left; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if err := db.Delete(ctx, "notthere"); err != nil {
		t.Fatal(err)
	}
}

func TestStats(t *testing.T) {
	ctx := context.Background()
	prefix := "/filesytem-prefix"
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname, prefix)
	if err != nil {
		t.Fatal(err)
	}

	t1, _ := time.Parse(time.RFC3339, "2023-08-10T10:00:02-08:00")
	t2, _ := time.Parse(time.RFC3339, "2023-08-11T10:00:02-08:00")
	times := []time.Time{t1, t2}
	payloads := []string{"foo", "bar"}
	for i, stats := range payloads {
		if err := db.SaveStats(ctx, times[i], []byte(stats)); err != nil {
			t.Errorf("got %v, want nil", err)
		}
	}

	when, stats, err := db.LastStats(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := string(stats), "bar"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := when, times[1]; !got.Equal(when) {
		t.Errorf("got %v, want %v", got, want)
	}

	scanTimes := []time.Time{}
	scanPayloads := []string{}
	err = db.VisitStats(ctx, time.Time{}, time.Now(),
		func(_ context.Context, when time.Time, detail []byte) bool {
			scanPayloads = append(scanPayloads, string(detail))
			scanTimes = append(scanTimes, when)
			return true
		})

	if got, want := scanPayloads, payloads; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := scanTimes, times; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

}
