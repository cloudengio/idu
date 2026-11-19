// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"gopkg.in/yaml.v3"
)

func writeRecordAllConfig(t *testing.T, dir, root string, recordAll ...string) iduconfig.T {
	t.Helper()
	cfgFile := filepath.Join(dir, "config-record-all.yaml")
	text := fmt.Sprintf("filesystems:\n  - root: %s\n    database: %s\n    hardlink_dedup: summaries\n",
		root, filepath.Join(dir, "db"))
	if len(recordAll) > 0 {
		text += "    hardlinks_record_all:\n"
		for _, p := range recordAll {
			text += "      - " + yamlQuote(p) + "\n"
		}
	}
	if err := os.WriteFile(cfgFile, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func runSuggest(ctx context.Context, t *testing.T, cfg iduconfig.T, sf *suggestFlags) string {
	t.Helper()
	globalConfig = cfg
	var out bytes.Buffer
	if err := (&statsCmds{}).suggestTo(ctx, &out, sf, nil); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestSuggestRecordAll(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := newHardlinkTree(t)
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")

	cfg := writeRecordAllConfig(t, tmpDir, root)
	analyzeWithConfig(ctx, t, cfg, root)
	if out := runSuggest(ctx, t, cfg, &suggestFlags{N: 20, Locate: true}); !strings.Contains(out, "no files with missing links") {
		t.Errorf("unexpected suggestions: %v", out)
	}

	// a/hl is a new link to b/h, which is unchanged and hence has a stale
	// link count and is counted twice.
	linkUnchanged(t, root)
	analyzeWithConfig(ctx, t, cfg, root)
	verifyHardlinkTree(ctx, t, cfg, root, false, 5, 3, 3)

	out := runSuggest(ctx, t, cfg, &suggestFlags{N: 20})
	if !strings.Contains(out, a+": 1 file, 1 missing link, eg. hl") || strings.Contains(out, "hardlinks_record_all:") {
		t.Errorf("unexpected suggestions: %v", out)
	}

	out = runSuggest(ctx, t, cfg, &suggestFlags{N: 20, Locate: true})
	if !strings.Contains(out, "# 1 file linked from "+a+"\n") {
		t.Errorf("unexpected suggestions: %v", out)
	}
	// The suggestions are valid YAML that matches b.
	var suggested struct {
		RecordAll []string `yaml:"hardlinks_record_all"`
	}
	if err := yaml.Unmarshal([]byte(out), &suggested); err != nil {
		t.Fatalf("%v: %v", out, err)
	}
	if len(suggested.RecordAll) != 1 || !regexp.MustCompile(suggested.RecordAll[0]).MatchString(b) {
		t.Fatalf("got %v, want a pattern matching %v", suggested.RecordAll, b)
	}

	// Adding the suggestion rewrites the summaries so that b records all of
	// its files and hence h is no longer counted twice.
	cfg = writeRecordAllConfig(t, tmpDir, root, suggested.RecordAll...)
	analyzeWithConfig(ctx, t, cfg, root)
	verifyHardlinkTree(ctx, t, cfg, root, true, 4, 4, 3)
	if out := runSuggest(ctx, t, cfg, &suggestFlags{N: 20, Locate: true}); !strings.Contains(out, "no files with missing links") {
		t.Errorf("unexpected suggestions: %v", out)
	}
}

func TestSuggestOutsideLinks(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := newHardlinkTree(t)
	// b/h has a link outside of the filesystem, which is not counted twice.
	if err := os.Link(filepath.Join(root, "b", "h"), filepath.Join(tmpDir, "outside")); err != nil {
		t.Fatal(err)
	}
	cfg := writeRecordAllConfig(t, tmpDir, root)
	analyzeWithConfig(ctx, t, cfg, root)
	out := runSuggest(ctx, t, cfg, &suggestFlags{N: 20, Locate: true})
	if !strings.Contains(out, "No prefixes with stale link counts were found") ||
		!strings.Contains(out, filepath.Join(root, "b")+": 1 file, 1 missing link, eg. h") ||
		strings.Contains(out, "hardlinks_record_all:") {
		t.Errorf("unexpected suggestions: %v", out)
	}

	// Rescan does not record hardlinks.
	rescan := writeHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupRescan)
	if out := runSuggest(ctx, t, rescan, &suggestFlags{}); !strings.Contains(out, "only detected when") {
		t.Errorf("unexpected suggestions: %v", out)
	}
}
