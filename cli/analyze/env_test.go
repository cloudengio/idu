// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
)

// testEnv is the environment used by the tests.
var testEnv = cli.NewEnv()

// openPrefixReadOnly is like cli.Env.OpenPrefixReadOnly but uses cfg.
func openPrefixReadOnly(ctx context.Context, cfg iduconfig.T, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error) {
	env := *testEnv
	env.Config = cfg
	return env.OpenPrefixReadOnly(ctx, prefix)
}

// countingProgress is a Progress that counts the events needed for the
// Summary recorded in the database log.
type countingProgress struct {
	mu     sync.Mutex
	s      Summary
	phases []string
}

func newCountingProgress(context.Context, bool) Progress {
	return &countingProgress{}
}

// newTestCommands returns Commands that use testEnv and countingProgress.
func newTestCommands() *Commands {
	return New(testEnv, WithProgress(newCountingProgress))
}

func (c *countingProgress) update(fn func(s *Summary)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(&c.s)
}

func (c *countingProgress) Before() time.Time { return time.Now() }
func (c *countingProgress) After(start time.Time) {
	c.update(func(s *Summary) { s.FSStats++; s.FSStatsTotal += int64(time.Since(start)) })
}
func (c *countingProgress) PrefixStarted() { c.update(func(s *Summary) { s.PrefixesStarted++ }) }
func (c *countingProgress) PrefixDone(deleted int, files int64) {
	c.update(func(s *Summary) { s.PrefixesFinished++; s.Files += files; s.PrefixesDeleted += int64(deleted) })
}
func (c *countingProgress) Error()             { c.update(func(s *Summary) { s.Errors++ }) }
func (c *countingProgress) ParentUnchanged()   { c.update(func(s *Summary) { s.ParentUnchanged++ }) }
func (c *countingProgress) ChildrenUnchanged() { c.update(func(s *Summary) { s.ChildrenUnchanged++ }) }
func (c *countingProgress) SlowScan()          { c.update(func(s *Summary) { s.SlowScans++ }) }
func (c *countingProgress) SyncScans(n int64) {
	c.update(func(s *Summary) { s.SynchronousScans = max(s.SynchronousScans, n) })
}
func (c *countingProgress) Restated(files, changed int) {
	c.update(func(s *Summary) { s.FilesRestated += int64(files); s.FilesRestatedChanged += int64(changed) })
}
func (c *countingProgress) Phase(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.phases = append(c.phases, name)
}
func (c *countingProgress) Stop()                  {}
func (c *countingProgress) Report(context.Context) {}
func (c *countingProgress) Summary() Summary {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.s
	if s.FSStats > 0 {
		s.FSStatMeanLatency = s.FSStatsTotal / s.FSStats
	}
	return s
}
