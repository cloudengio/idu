// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"sync"

	"cloudeng.io/errors"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/os/userid"
)

type databaseManager struct {
	sync.Mutex
	dbs map[string]filewalk.Database
}

var globalDatabaseManager = databaseManager{
	dbs: map[string]filewalk.Database{},
}

func (dbm *databaseManager) DatabaseFor(ctx context.Context, prefix string, opts ...filewalk.DatabaseOption) (filewalk.Database, error) {
	dbm.Lock()
	defer dbm.Unlock()
	cfg, ok := globalConfig.DatabaseFor(prefix)
	if !ok {
		return nil, fmt.Errorf("no database is configured for %v", prefix)
	}
	if db, ok := dbm.dbs[cfg.Prefix]; ok {
		return db, nil
	}
	db, err := cfg.Open(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open database for %v: %v", prefix, err)
	}
	dbm.dbs[cfg.Prefix] = db
	debug(ctx, 1, "prefix: %v: using database %v\n", prefix, cfg.Description)
	return db, nil
}

func (dbm *databaseManager) Set(ctx context.Context, prefix string, info *filewalk.PrefixInfo, opts ...filewalk.DatabaseOption) error {
	db, err := dbm.DatabaseFor(ctx, prefix, opts...)
	if err != nil {
		return err
	}
	return db.Set(ctx, prefix, info)
}

func (dbm *databaseManager) Get(ctx context.Context, prefix string, info *filewalk.PrefixInfo, opts ...filewalk.DatabaseOption) (bool, error) {
	db, err := dbm.DatabaseFor(ctx, prefix, opts...)
	if err != nil {
		return false, err
	}
	return db.Get(ctx, prefix, info)
}

func (dbm *databaseManager) Close(ctx context.Context) error {
	dbm.Lock()
	defer dbm.Unlock()
	errs := errors.M{}
	for _, db := range dbm.dbs {
		errs.Append(db.Close(ctx))
	}
	dbm.dbs = map[string]filewalk.Database{}
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

func (um *userManager) nameForPrefix(ctx context.Context, db filewalk.Database, prefix string) string {
	var pi filewalk.PrefixInfo
	ok, err := db.Get(ctx, prefix, &pi)
	if err != nil || !ok {
		return "(unknown)"
	}
	return um.nameForUID(pi.UserID)
}
