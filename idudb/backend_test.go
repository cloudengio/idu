// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
)

var withBadger = idudb.WithBackendFactory(idudb.NewBackendFactory(badgerdb.NewBackend))

func TestOpen(t *testing.T) {
	ctx := context.Background()
	cfg := &iduconfig.Settings{Database: filepath.Join(t.TempDir(), "db")}

	wdb, err := idudb.OpenWritable(ctx, cfg, []idudb.OpenOption{withBadger})
	if err != nil {
		t.Fatal(err)
	}
	if err := wdb.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rdb, err := idudb.Open(ctx, cfg, withBadger)
	if err != nil {
		t.Fatal(err)
	}
	if err := idudb.CheckSummaryKeys(ctx, rdb); err != nil {
		t.Error(err)
	}
	if err := rdb.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOpenLegacy(t *testing.T) {
	ctx := context.Background()
	cfg := &iduconfig.Settings{Database: filepath.Join(t.TempDir(), "db")}
	raw, err := idudb.OpenBackend(ctx, cfg.Database, false, withBadger)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Set(ctx, "/p", []byte("val"), false); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = idudb.Open(ctx, cfg, withBadger)
	if !errors.Is(err, idudb.ErrLegacyDatabase) || !strings.Contains(err.Error(), cfg.Database) {
		t.Errorf("Open: got %v, want ErrLegacyDatabase for %v", err, cfg.Database)
	}
	_, err = idudb.OpenWritable(ctx, cfg, []idudb.OpenOption{withBadger})
	if !errors.Is(err, idudb.ErrLegacyDatabase) || !strings.Contains(err.Error(), cfg.Database) {
		t.Errorf("OpenWritable: got %v, want ErrLegacyDatabase for %v", err, cfg.Database)
	}
}

func TestOpenBackendOptions(t *testing.T) {
	ctx := context.Background()
	if _, err := idudb.OpenBackend(ctx, "x", true); !errors.Is(err, idudb.ErrNoBackend) {
		t.Errorf("got %v, want %v", err, idudb.ErrNoBackend)
	}
	wantErr := errors.New("open failed")
	failing := func(context.Context, string, bool) (idudb.Database, error) {
		return nil, wantErr
	}
	if _, err := idudb.OpenBackend(ctx, "x", true, idudb.WithBackendFactory(failing)); !errors.Is(err, wantErr) {
		t.Errorf("got %v, want %v", err, wantErr)
	}

	if _, err := idudb.Open(ctx, nil, withBadger); err == nil {
		t.Error("expected an error for nil settings in Open")
	}
	if _, err := idudb.OpenWritable(ctx, nil, []idudb.OpenOption{withBadger}); err == nil {
		t.Error("expected an error for nil settings in OpenWritable")
	}
	if _, err := idudb.OpenBackend(ctx, "x", true, idudb.WithBackendFactory(idudb.NewBackendFactory[*badgerdb.Database](nil))); !errors.Is(err, idudb.ErrNoBackend) {
		t.Errorf("got %v, want %v", err, idudb.ErrNoBackend)
	}
	nilPtrFactory := func(context.Context, string, bool) (*badgerdb.Database, error) {
		return nil, wantErr
	}
	if db, err := idudb.NewBackendFactory(nilPtrFactory)(ctx, "x", true); !errors.Is(err, wantErr) || db != nil {
		t.Errorf("got (%v, %v), want (nil, %v)", db, err, wantErr)
	}

	for _, progress := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			dbDir := filepath.Join(t.TempDir(), "db")
			slow := func(ctx context.Context, path string, readonly bool) (idudb.Database, error) {
				time.Sleep(1100 * time.Millisecond)
				return idudb.NewBackendFactory(badgerdb.NewBackend)(ctx, path, readonly)
			}
			var out bytes.Buffer
			opts := []idudb.OpenOption{idudb.WithBackendFactory(slow)}
			if progress {
				opts = append(opts, idudb.WithOpenProgress(&out))
			}
			db, err := idudb.OpenBackend(ctx, dbDir, false, opts...)
			if err != nil {
				t.Fatal(err)
			}
			_ = db.Close(ctx)
			if got := strings.Contains(out.String(), "waiting for database to open"); got != progress {
				t.Errorf("progress %v: output %q", progress, out.String())
			}
		})
	}
}
