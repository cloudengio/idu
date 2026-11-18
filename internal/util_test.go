// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"flag"
	"io/fs"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
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

}

func TestNegativeSince(t *testing.T) {
	t.Parallel()

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
