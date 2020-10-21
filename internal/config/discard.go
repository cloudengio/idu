// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"context"

	"cloudeng.io/file/filewalk"
)

type nullDatabase struct{}

func (ndb *nullDatabase) Set(ctx context.Context, prefix string, info *filewalk.PrefixInfo) error {
	return nil
}

func (ndb *nullDatabase) Get(ctx context.Context, prefix string, info *filewalk.PrefixInfo) (bool, error) {
	return false, nil
}
func (ndb *nullDatabase) Save(ctx context.Context) error {
	return nil
}

func (ndb *nullDatabase) Close(ctx context.Context) error {
	return nil
}
func (ndb *nullDatabase) UserIDs(ctx context.Context) ([]string, error) {
	return nil, nil
}
func (ndb *nullDatabase) Metrics() []filewalk.MetricName {
	return nil
}
func (ndb *nullDatabase) Total(ctx context.Context, name filewalk.MetricName, opts ...filewalk.MetricOption) (int64, error) {
	return int64(0), nil
}

func (ndb *nullDatabase) TopN(ctx context.Context, name filewalk.MetricName, n int, opts ...filewalk.MetricOption) ([]filewalk.Metric, error) {
	return nil, nil
}
func (ndb *nullDatabase) NewScanner(prefix string, limit int, opts ...filewalk.ScannerOption) filewalk.DatabaseScanner {
	return ndb
}

func (ndb *nullDatabase) Scan(ctx context.Context) bool {
	return false
}

func (ndb *nullDatabase) PrefixInfo() (string, *filewalk.PrefixInfo) {
	return "", nil
}

func (ndb *nullDatabase) Err() error {
	return nil
}
