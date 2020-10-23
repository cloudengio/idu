// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"context"
	"fmt"
	"os"

	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/localdb"
)

type databaseSpec struct {
	Prefix string      `yaml:"prefix" cmd:"use this database for this prefix"`
	Type   string      `yaml:"type" cmd:"type of database to be used"`
	config interface{} `yaml:"custom fields" cmd:"database specific configuration fields"` //nolint:structcheck
}

type database struct {
	Spec        databaseSpec `yaml:",inline"`
	open        DatabaseOpenFunc
	delete      DatabaseDeleteFunc
	description string
}

type databaseFactory func(spec interface{}) (DatabaseOpenFunc, DatabaseDeleteFunc, string)

type databaseConfig struct {
	config  interface{}
	factory databaseFactory
}

var supportedDatabases = map[string]databaseConfig{
	"local": {&localDatabaseSpec{}, localOpen},
}

type localDatabaseSpec struct {
	Directory string `yaml:"directory" cmd:"local directory containing the database"`
}

func (d *database) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if err := unmarshal(&d.Spec); err != nil {
		return err
	}
	cfg, ok := supportedDatabases[d.Spec.Type]
	if !ok {
		return fmt.Errorf("unsupported database: %q", d.Spec.Type)
	}
	if err := unmarshal(cfg.config); err != nil {
		return err
	}
	d.open, d.delete, d.description = cfg.factory(cfg.config)
	return nil
}

func localOpen(spec interface{}) (DatabaseOpenFunc, DatabaseDeleteFunc, string) {
	dir := os.ExpandEnv(spec.(*localDatabaseSpec).Directory)
	open := func(ctx context.Context, opts ...filewalk.DatabaseOption) (filewalk.Database, error) {
		return localdb.Open(ctx, dir, opts)
	}
	delete := func(ctx context.Context) error {
		return os.RemoveAll(dir)
	}
	return open, delete, fmt.Sprintf("local database in %s", dir)
}
