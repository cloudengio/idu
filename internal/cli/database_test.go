// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
)

func TestOpenDatabaseErrorsAndSlowOpen(t *testing.T) {
	origFactory := testEnv.DatabaseFactory
	t.Cleanup(func() {
		testEnv.DatabaseFactory = origFactory
	})

	t.Run("factory error", func(t *testing.T) {
		wantErr := errors.New("open failed")
		testEnv.DatabaseFactory = func(context.Context, string, bool) (idudb.Database, error) {
			return nil, wantErr
		}
		if _, err := testEnv.OpenDatabase(context.Background(), "/tmp/x", false); !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
	})

	t.Run("canceled while opening", func(t *testing.T) {
		release := make(chan struct{})
		returned := make(chan struct{})
		testEnv.DatabaseFactory = func(context.Context, string, bool) (idudb.Database, error) {
			defer close(returned)
			<-release
			return nil, errors.New("canceled")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := testEnv.OpenDatabase(ctx, "/tmp/x", false); !errors.Is(err, context.Canceled) {
			close(release)
			<-returned
			t.Fatalf("got %v, want context.Canceled", err)
		}
		close(release)
		<-returned
	})

	t.Run("slow open prints waiting message", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			dbDir := filepath.Join(t.TempDir(), "slow-db")
			testEnv.DatabaseFactory = func(ctx context.Context, path string, readonly bool) (idudb.Database, error) {
				time.Sleep(1100 * time.Millisecond)
				return origFactory(ctx, path, readonly)
			}
			var out bytes.Buffer
			origOut := testEnv.OpenProgress
			testEnv.OpenProgress = &out
			defer func() { testEnv.OpenProgress = origOut }()
			db, err := testEnv.OpenDatabase(context.Background(), dbDir, false)
			if err != nil {
				t.Fatalf("OpenDatabase: %v", err)
			}
			_ = db.Close(context.Background())
			if !strings.Contains(out.String(), "waiting for database to open") {
				t.Errorf("missing waiting message: %q", out.String())
			}
		})
	})
}

func TestOpenDatabaseCanceledClosesDB(t *testing.T) {
	origFactory := testEnv.DatabaseFactory
	t.Cleanup(func() { testEnv.DatabaseFactory = origFactory })

	dbDir := t.TempDir()
	raw, err := origFactory(context.Background(), dbDir, false)
	if err != nil {
		t.Fatal(err)
	}
	closedCh := make(chan struct{})
	fdb := &closeNotifyDB{Database: raw, closedCh: closedCh}

	unblock := make(chan struct{})
	started := make(chan struct{})
	testEnv.DatabaseFactory = func(context.Context, string, bool) (idudb.Database, error) {
		close(started)
		<-unblock
		return fdb, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	_, err = testEnv.OpenDatabase(ctx, dbDir, false)
	close(unblock)
	if !errors.Is(err, context.Canceled) {
		_ = raw.Close(context.Background())
		t.Fatalf("OpenDatabase err = %v, want context.Canceled", err)
	}
	select {
	case <-closedCh:
	case <-time.After(2 * time.Second):
		_ = raw.Close(context.Background())
		t.Fatal("leaked opened DB after OpenDatabase context cancellation")
	}
}

type closeNotifyDB struct {
	idudb.Database
	closedCh  chan struct{}
	closeOnce sync.Once
}

func (c *closeNotifyDB) Close(ctx context.Context) error {
	err := c.Database.Close(ctx)
	c.closeOnce.Do(func() {
		close(c.closedCh)
	})
	return err
}

func TestOpenPrefixReadOnlyPreservesErrorChain(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "legacy-db")
	raw, err := testEnv.OpenDatabase(ctx, dbDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Set(ctx, "/p", []byte("val"), false); err != nil {
		_ = raw.Close(ctx)
		t.Fatal(err)
	}
	if err := raw.Close(ctx); err != nil {
		t.Fatal(err)
	}

	cfg, err := iduconfig.ParseConfig([]byte("filesystems:\n  - name: legacy\n    prefix: /p\n    database: " + dbDir + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, rdb, err := openPrefixReadOnly(ctx, cfg, "legacy")
	if !errors.Is(err, idudb.ErrLegacyDatabase) {
		if rdb != nil {
			_ = rdb.Close(ctx)
		}
		t.Fatalf("openPrefixReadOnly err = %v, want wrapped idudb.ErrLegacyDatabase", err)
	}
}
