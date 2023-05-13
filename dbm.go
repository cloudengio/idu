// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"sync"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/errors"
	"cloudeng.io/os/userid"
)

type databaseManager struct {
	sync.Mutex
	dbs map[string]internal.Database
}

var globalDatabaseManager = databaseManager{
	dbs: map[string]internal.Database{},
}

func (dbm *databaseManager) DatabaseFor(ctx context.Context, prefix string, opts ...internal.DatabaseOption) (internal.Database, error) {
	dbm.Lock()
	defer dbm.Unlock()
	db, _, err := dbm.databaseForLocked(ctx, prefix, opts...)
	return db, err
}

func (dbm *databaseManager) databaseForLocked(ctx context.Context, prefix string, opts ...internal.DatabaseOption) (internal.Database, config.Database, error) {
	cfg, ok := globalConfig.DatabaseFor(prefix)
	if !ok {
		return nil, cfg, fmt.Errorf("no database is configured for %v", prefix)
	}
	if db, ok := dbm.dbs[cfg.Prefix]; ok {
		return db, cfg, nil
	}
	db, err := cfg.Open(ctx, opts...)
	if err != nil {
		return nil, cfg, fmt.Errorf("failed to open database for %v: %v", prefix, err)
	}
	dbm.dbs[cfg.Prefix] = db
	debug(ctx, 1, "prefix: %v: using database %v\n", prefix, cfg.Description)
	return db, cfg, nil
}

func (dbm *databaseManager) Set(ctx context.Context, prefix string, info *internal.PrefixInfo, opts ...internal.DatabaseOption) error {
	db, err := dbm.DatabaseFor(ctx, prefix, opts...)
	if err != nil {
		return err
	}
	return db.Set(ctx, prefix, info)
}

func (dbm *databaseManager) Get(ctx context.Context, prefix string, info *internal.PrefixInfo, opts ...internal.DatabaseOption) (bool, error) {
	db, err := dbm.DatabaseFor(ctx, prefix, opts...)
	if err != nil {
		return false, err
	}
	return db.Get(ctx, prefix, info)
}

func (dbm *databaseManager) Delete(ctx context.Context, separator, prefix string, prefixes []string, opts ...internal.DatabaseOption) (int, error) {
	db, err := dbm.DatabaseFor(ctx, prefix, opts...)
	if err != nil {
		return 0, err
	}
	return db.Delete(ctx, separator, prefixes, true)
}

func (dbm *databaseManager) Compact(ctx context.Context, prefix string) error {
	dbm.Lock()
	defer dbm.Unlock()
	db, cfg, err := dbm.databaseForLocked(ctx, prefix)
	if err != nil {
		return err
	}
	delete(dbm.dbs, cfg.Prefix)
	return db.CompactAndClose(ctx)
}

func (dbm *databaseManager) Close(ctx context.Context, prefix string) error {
	dbm.Lock()
	defer dbm.Unlock()
	db, cfg, err := dbm.databaseForLocked(ctx, prefix)
	if err != nil {
		return err
	}
	delete(dbm.dbs, cfg.Prefix)
	return db.Close(ctx)
}

func (dbm *databaseManager) CloseAll(ctx context.Context) error {
	dbm.Lock()
	defer dbm.Unlock()
	errs := errors.M{}
	for _, db := range dbm.dbs {
		errs.Append(db.Close(ctx))
	}
	dbm.dbs = map[string]internal.Database{}
	return errs.Err()
}

type userManager struct {
	idmanager *userid.IDManager
}

var globalUserManager = userManager{
	idmanager: userid.NewIDManager(),
}

func (um *userManager) nameForUID(uid string) string {
	info, err := um.idmanager.LookupUser(uid)
	if err == nil {
		return info.Username
	}
	return uid
}

func (um *userManager) uidForName(name string) string {
	info, err := um.idmanager.LookupUser(name)
	if err == nil {
		return info.UID
	}
	return name
}

func (um *userManager) gidForName(name string) string {
	grp, err := um.idmanager.LookupGroup(name)
	if err == nil {
		return grp.Gid
	}
	return name
}

func (um *userManager) nameForGID(gid string) string {
	grp, err := um.idmanager.LookupGroup(gid)
	if err == nil {
		return grp.Name
	}
	return gid
}

func (um *userManager) nameForPrefix(ctx context.Context, db internal.Database, prefix string) string {
	var pi internal.PrefixInfo
	ok, err := db.Get(ctx, prefix, &pi)
	if err != nil || !ok {
		return "(unknown)"
	}
	return um.nameForUID(pi.UserID)
}
