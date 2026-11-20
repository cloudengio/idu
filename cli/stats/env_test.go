// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"context"
	"testing"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/cli/internal/snapshot"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/file/localfs"
)

// testEnv is the environment used by the tests.
var testEnv = cli.NewEnv()

// openPrefixReadOnly is like cli.Env.OpenPrefixReadOnly but uses cfg.
func openPrefixReadOnly(ctx context.Context, cfg iduconfig.T, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error) {
	env := *testEnv
	env.Config = cfg
	return env.OpenPrefixReadOnly(ctx, prefix)
}

func analyzeWithConfig(ctx context.Context, t *testing.T, cfg iduconfig.T, root string) {
	t.Helper()
	testEnv.Config = cfg
	if err := analyze.New(testEnv).Analyze(ctx, &analyze.Flags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
}

// checkRootTotals checks the number of files and hardlinks for root
// computed from the summaries and, if exact is true, that they match those
// computed by a rescan of all of the file metadata.
func checkRootTotals(ctx context.Context, t *testing.T, cfg iduconfig.T, root string, exact bool, files, hardlinks int64) {
	t.Helper()
	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	c := snapshot.Computer{}
	fromSummaries, err := c.FromSummaries(ctx, db, pcfg, root, 10)
	if err != nil {
		t.Fatal(err)
	}
	match, err := snapshot.NewMatcher(ctx, localfs.New(), pcfg)
	if err != nil {
		t.Fatal(err)
	}
	fromPrefixes, err := c.FromPrefixes(ctx, db, pcfg, match, root, 10)
	if err != nil {
		t.Fatal(err)
	}
	got, rescan := fromSummaries.Prefix, fromPrefixes.Prefix
	if got.TotalFiles != files || got.TotalHardlinks != hardlinks {
		t.Errorf("got files=%v hardlinks=%v, want %v %v", got.TotalFiles, got.TotalHardlinks, files, hardlinks)
	}
	if exact && (got.TotalFiles != rescan.TotalFiles || got.TotalBytes != rescan.TotalBytes || got.TotalHardlinks != rescan.TotalHardlinks) {
		t.Errorf("summaries: files=%v bytes=%v hardlinks=%v, rescan: %v %v %v", got.TotalFiles, got.TotalBytes, got.TotalHardlinks, rescan.TotalFiles, rescan.TotalBytes, rescan.TotalHardlinks)
	}
}
