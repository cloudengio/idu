// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type userInfo struct {
	ID   int64
	GIDS []int64
}

type groupInfo struct {
	GID int64
}

func (db *Database) SetUser(_ context.Context, user string, id int64, gids []int64) error {
	u := userInfo{
		ID:   id,
		GIDS: gids,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&u); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.prefix))
		if b == nil {
			return fmt.Errorf("no bucket for prefix: %v", db.prefix)
		}
		return b.Bucket([]byte(bucketUsers)).Put([]byte(user), buf.Bytes())
	})
}

func (db *Database) VisitUsers(ctx context.Context, user string, visitor func(ctx context.Context, user string, id int64, gids []int64) bool) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketUsers, user)
		if err != nil {
			return err
		}
		for ; k != nil; k, v = cursor.Next() {
			var u userInfo
			if err := gob.NewDecoder(bytes.NewBuffer(v)).Decode(&u); err != nil {
				return err
			}
			if !visitor(ctx, string(k), u.ID, u.GIDS) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}

func (db *Database) SetGroup(_ context.Context, group string, gid int64) error {
	g := groupInfo{
		GID: gid,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&g); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.prefix))
		if b == nil {
			return fmt.Errorf("no bucket for prefix: %v", db.prefix)
		}
		return b.Bucket([]byte(bucketGroups)).Put([]byte(group), buf.Bytes())
	})
}

func (db *Database) VisitGroups(ctx context.Context, group string, visitor func(ctx context.Context, group string, gid int64) bool) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketGroups, group)
		if err != nil {
			return err
		}
		for ; k != nil; k, v = cursor.Next() {
			var g groupInfo
			if err := gob.NewDecoder(bytes.NewBuffer(v)).Decode(&g); err != nil {
				return err
			}
			if !visitor(ctx, string(k), g.GID) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}
