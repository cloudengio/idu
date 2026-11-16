// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"context"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file"
)

func TestTimeRange(t *testing.T) {

	parse := func(args ...string) (time.Time, time.Time) {
		fs := &flag.FlagSet{}
		var trf internal.TimeRangeFlags
		if err := flags.RegisterFlagsInStruct(fs, "subcmd", &trf, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		from, to, set, err := trf.FromTo()
		if err != nil {
			t.Fatal(err)
		}
		if !set {
			t.Fatal("expected set to be true")
		}
		return from, to
	}

	from, to := parse("--since=1h")
	if got := to.Sub(from); got < time.Minute*55 || got > time.Hour {
		t.Errorf("out of plausible range %v", got)
	}
	from, to = parse("--from=2020-10-10", "-to=2020-10-11")
	if got, want := to.Sub(from), time.Hour*24; got != want {
		t.Errorf("got %v, want 24h", got)
	}

}

const simple = `filesystems:
  - name: mytmp
    prefix: /tmp
  - name: root
    prefix: /
`

func TestPrefixLookup(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}

	_, prefix, target, err := internal.LookupPrefix(ctx, cfg, "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prefix.Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := target, "/tmp"; got != want {
		t.Errorf("got target %v, want %v", got, want)
	}

	_, prefix, target, err = internal.LookupPrefix(ctx, cfg, "/tmp/xx")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prefix.Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := target, "/tmp/xx"; got != want {
		t.Errorf("got target %v, want %v", got, want)
	}

	_, prefix, target, err = internal.LookupPrefix(ctx, cfg, "mytmp")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prefix.Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := target, "/tmp"; got != want {
		t.Errorf("got target %v, want %v", got, want)
	}

	_, prefix, target, err = internal.LookupPrefix(ctx, cfg, "root")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prefix.Prefix, "/"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := target, "/"; got != want {
		t.Errorf("got target %v, want %v", got, want)
	}
}

func TestPrefixLookupRelative(t *testing.T) {
	ctx := context.Background()
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []string{".", "", "./"} {
		here := "filesystems:\n  - prefix: " + pwd + "\n"
		cfg, err := config.ParseConfig([]byte(here))
		if err != nil {
			t.Fatal(err)
		}
		_, prefix, _, err := internal.LookupPrefix(ctx, cfg, tc)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := prefix.Prefix, pwd; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		_, prefix, _, err = internal.LookupPrefix(ctx, cfg, "config")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := prefix.Prefix, pwd; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

	}
}

func TestTimeRangeEdgesAndErrors(t *testing.T) {
	t.Parallel()

	t.Run("unset returns set=false", func(t *testing.T) {
		t.Parallel()
		var trf internal.TimeRangeFlags
		fs := &flag.FlagSet{}
		if err := flags.RegisterFlagsInStruct(fs, "subcmd", &trf, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := fs.Parse(nil); err != nil {
			t.Fatal(err)
		}
		from, to, set, err := trf.FromTo()
		if err != nil {
			t.Fatal(err)
		}
		if set || !from.IsZero() || to.IsZero() {
			t.Errorf("got (%v, %v, %v), want (zero, non-zero, false)", from, to, set)
		}
	})

	t.Run("from only leaves to near now", func(t *testing.T) {
		t.Parallel()
		var trf internal.TimeRangeFlags
		fs := &flag.FlagSet{}
		if err := flags.RegisterFlagsInStruct(fs, "subcmd", &trf, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := fs.Parse([]string{"--from=2020-01-02"}); err != nil {
			t.Fatal(err)
		}
		before := time.Now()
		from, to, set, err := trf.FromTo()
		if err != nil {
			t.Fatal(err)
		}
		if !set || from.Year() != 2020 || to.Before(before) {
			t.Errorf("got (%v, %v, %v)", from, to, set)
		}
	})

	t.Run("LookupPrefix missing config returns error", func(t *testing.T) {
		t.Parallel()
		cfg, err := config.ParseConfig([]byte("filesystems:\n  - name: mytmp\n    prefix: /tmp\n"))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := internal.LookupPrefix(context.Background(), cfg, "/nonexistent/path"); err == nil {
			t.Fatal("expected error for unmatched prefix")
		}
	})
}

func TestLookupPrefixCleanAndNegativeSince(t *testing.T) {
	t.Parallel()

	t.Run("LookupPrefix cleans trailing slash", func(t *testing.T) {
		t.Parallel()
		root := filepath.Join(t.TempDir(), "tree")
		cfg, err := config.ParseConfig([]byte("filesystems:\n  - name: mytree\n    prefix: " + root + string(filepath.Separator) + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		sub := filepath.Join(root, "sub")
		for _, in := range []string{sub + string(filepath.Separator), "mytree"} {
			_, _, target, err := internal.LookupPrefix(context.Background(), cfg, in)
			if err != nil {
				t.Fatalf("LookupPrefix(%q): %v", in, err)
			}
			if target != filepath.Clean(target) {
				t.Errorf("LookupPrefix(%q) target = %q, want cleaned %q", in, target, filepath.Clean(target))
			}
		}
	})

	t.Run("negative since returns error", func(t *testing.T) {
		t.Parallel()
		var trf internal.TimeRangeFlags
		fs := &flag.FlagSet{}
		if err := flags.RegisterFlagsInStruct(fs, "subcmd", &trf, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := fs.Parse([]string{"--since=-1h"}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := trf.FromTo(); err == nil {
			t.Fatal("expected error for negative --since duration")
		}
	})
}

func TestPrefixInfoAsFSInfo(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	tests := []struct {
		name      string
		mode      fs.FileMode
		wantIsDir bool
	}{
		{name: "directory", mode: fs.ModeDir | 0o755, wantIsDir: true},
		{name: "regular file", mode: 0o644, wantIsDir: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := file.NewInfo("ignored", 42, tc.mode, now, file.XAttr{UID: 1, GID: 2})
			pi := prefixinfo.New("/some/dir", raw)
			fi := internal.PrefixInfoAsFSInfo(pi, "/some/dir")
			if fi.Name() != "/some/dir" {
				t.Errorf("Name() = %q, want /some/dir", fi.Name())
			}
			if fi.IsDir() != tc.wantIsDir {
				t.Errorf("IsDir() = %v, want %v", fi.IsDir(), tc.wantIsDir)
			}
			if fi.Size() != 42 || fi.Mode() != tc.mode || !fi.ModTime().Equal(now) {
				t.Errorf("unexpected metadata: size=%d mode=%v modtime=%v", fi.Size(), fi.Mode(), fi.ModTime())
			}
			if fi.Sys() != nil {
				t.Errorf("Sys() = %v, want nil", fi.Sys())
			}
		})
	}
}
