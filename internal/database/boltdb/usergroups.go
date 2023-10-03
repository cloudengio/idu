// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type userInfo struct {
	User string
	GIDS []uint32
}

type groupInfo struct {
	Group string
}

func itob(v uint32) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func (db *Database) SetUser(_ context.Context, id uint32, user string, gids []uint32) error {
	u := userInfo{
		User: user,
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
		return b.Bucket([]byte(bucketUsers)).Put(itob(id), buf.Bytes())
	})
}

func (db *Database) VisitUsers(ctx context.Context, user string, visitor func(ctx context.Context, id uint32, user string, gids []uint32) bool) error {
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
			id := binary.BigEndian.Uint32(k)
			if !visitor(ctx, id, u.User, u.GIDS) {
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

func (db *Database) SetGroup(_ context.Context, gid uint32, group string) error {
	g := groupInfo{
		Group: group,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&g); err != nil {
		return err
	}
	id := itob(gid)
	return db.bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.prefix))
		if b == nil {
			return fmt.Errorf("no bucket for prefix: %v", db.prefix)
		}
		return b.Bucket([]byte(bucketGroups)).Put(id, buf.Bytes())
	})
}

func (db *Database) VisitGroups(ctx context.Context, group string, visitor func(ctx context.Context, gid uint32, group string) bool) error {
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
			id := binary.BigEndian.Uint32(k)
			if !visitor(ctx, id, g.Group) {
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
