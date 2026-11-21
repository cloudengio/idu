// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package mcpserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb/defaultdb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/mcpserver"
	"cloudeng.io/cmd/idu/usernames"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// analyzed analyzes the hardlink tree, see clitest.CreateHardlinkTree,
// with setup, if not nil, called first, and returns its configuration and
// root.
func analyzed(t *testing.T, setup func(root string)) (iduconfig.T, string) {
	t.Helper()
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)
	if setup != nil {
		setup(root)
	}
	cfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	env := cli.NewEnv()
	env.Config = cfg
	env.OpenProgress = io.Discard
	if err := analyze.New(env, analyze.WithOutput(io.Discard)).Analyze(ctx, &analyze.Flags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
	return cfg, root
}

// connect returns a client session connected to a server for cfg.
func connect(t *testing.T, cfg iduconfig.T, opts ...mcpserver.Option) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	srv := mcpserver.New(cfg, append([]mcpserver.Option{mcpserver.WithOpenOptions(defaultdb.OpenOptions()...)}, opts...)...)
	st, ct := mcp.NewInMemoryTransports()
	if _, err := srv.MCPServer().Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// call calls tool with args and decodes its structured result into out,
// it returns the text of the result if the tool reported an error.
func call(t *testing.T, cs *mcp.ClientSession, tool string, args any, out any) string {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%v: %v", tool, err)
	}
	if res.IsError {
		var msgs []string
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				msgs = append(msgs, tc.Text)
			}
		}
		return strings.Join(msgs, "\n")
	}
	buf, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(buf, out); err != nil {
		t.Fatalf("%v: %v: %s", tool, err, buf)
	}
	return ""
}

func mustCall(t *testing.T, cs *mcp.ClientSession, tool string, args any, out any) {
	t.Helper()
	if msg := call(t, cs, tool, args, out); msg != "" {
		t.Fatalf("%v: %v", tool, msg)
	}
}

func TestTools(t *testing.T) {
	cfg, root := analyzed(t, nil)
	cs := connect(t, cfg)
	checkToolList(t, cs)

	var fss mcpserver.Filesystems
	mustCall(t, cs, "list_filesystems", map[string]any{}, &fss)
	if len(fss.Filesystems) != 1 || fss.Filesystems[0].Location != root || fss.Filesystems[0].Status != "ok" || fss.Filesystems[0].LastAnalyzed.IsZero() {
		t.Errorf("unexpected filesystems: %+v", fss)
	}
	aBytes := checkUsage(t, cs, root)
	checkChildren(t, cs, root)
	testTopN(t, cs, root, aBytes)

	var errs mcpserver.Errors
	mustCall(t, cs, "errors", map[string]any{}, &errs)
	if len(errs.Errors) != 0 || errs.More {
		t.Errorf("unexpected errors: %+v", errs)
	}
}

func checkToolList(t *testing.T, cs *mcp.ClientSession) {
	t.Helper()
	var tools []string
	for tool, err := range cs.Tools(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		if !tool.Annotations.ReadOnlyHint {
			t.Errorf("%v is not read-only", tool.Name)
		}
		tools = append(tools, tool.Name)
	}
	slices.Sort(tools)
	if want := []string{"children", "errors", "list_filesystems", "top_n", "usage"}; !slices.Equal(tools, want) {
		t.Errorf("got %v, want %v", tools, want)
	}
}

// checkUsage checks the usage tool and returns the size of a's files.
func checkUsage(t *testing.T, cs *mcp.ClientSession, root string) int64 {
	t.Helper()
	// Files with more than one link are counted once: there are 4
	// distinct files in a, ab, a-b, b and b/c.
	var usage mcpserver.Usage
	mustCall(t, cs, "usage", map[string]any{}, &usage)
	if usage.Path != root || usage.Recursive == nil || usage.Recursive.Files != 4 || usage.Recursive.Prefixes != 5 || usage.Direct.Prefixes != 4 {
		t.Errorf("unexpected usage: %+v %+v", usage, usage.Recursive)
	}
	var aUsage mcpserver.Usage
	mustCall(t, cs, "usage", map[string]any{"prefix": "a"}, &aUsage)
	if aUsage.Path != filepath.Join(root, "a") || aUsage.Direct.Files != 2 || aUsage.Direct.Bytes < 1500 {
		t.Errorf("unexpected usage: %+v", aUsage)
	}
	return aUsage.Direct.Bytes
}

func checkChildren(t *testing.T, cs *mcp.ClientSession, root string) {
	t.Helper()
	var children mcpserver.Children
	mustCall(t, cs, "children", map[string]any{"metric": "bytes", "n": 3}, &children)
	if children.Total != 4 || len(children.Children) != 3 || children.Metric != "bytes" ||
		children.Children[0].Path != filepath.Join(root, "a") {
		t.Errorf("unexpected children: %+v", children)
	}
	for i := 1; i < len(children.Children); i++ {
		if children.Children[i].Recursive.Bytes > children.Children[i-1].Recursive.Bytes {
			t.Errorf("children are not ordered by bytes: %+v", children.Children)
		}
	}
}

func testTopN(t *testing.T, cs *mcp.ClientSession, root string, aBytes int64) {
	t.Helper()
	const (
		stored   = "the rankings maintained by analyze"
		computed = "computed by a pass over all of the summaries beneath the prefix"
	)
	user := usernames.Manager.NameForUID(int64(os.Getuid()))
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	for _, tc := range []struct {
		args   map[string]any
		n      int
		source string
		// first describes the expected first entry, any value that is
		// not set is not checked.
		first mcpserver.Entry
	}{
		{map[string]any{"n": 2}, 2, stored, mcpserver.Entry{Path: root}},
		{map[string]any{"by": "prefix", "metric": "bytes", "n": 1}, 1, stored, mcpserver.Entry{Path: a, Value: aBytes}},
		{map[string]any{"by": "prefix", "metric": "bytes", "user": user, "n": 1}, 1, stored, mcpserver.Entry{Path: a}},
		{map[string]any{"by": "user"}, 1, stored, mcpserver.Entry{User: user}},
		// Rankings of prefixes, users and groups beneath a prefix other
		// than the root are computed from the summaries.
		{map[string]any{"by": "group", "prefix": "b"}, 1, computed, mcpserver.Entry{}},
		// b/c contains only a link to a file counted in a, and hence no
		// files.
		{map[string]any{"prefix": "b", "metric": "files"}, 1, stored, mcpserver.Entry{Path: b, Value: 1}},
	} {
		var top mcpserver.TopN
		mustCall(t, cs, "top_n", tc.args, &top)
		if len(top.Entries) != tc.n || top.Source != tc.source || !top.Exact {
			t.Errorf("%v: unexpected top_n: %+v", tc.args, top)
			continue
		}
		got, want := top.Entries[0], tc.first
		if (want.Path != "" && got.Path != want.Path) || (want.Value != 0 && got.Value != want.Value) ||
			(want.User != "" && (got.User != want.User || got.ID == nil)) {
			t.Errorf("%v: got %+v, want %+v", tc.args, got, want)
		}
		if tc.args["by"] == "group" && (got.Group == "" || got.ID == nil) {
			t.Errorf("%v: got %+v", tc.args, got)
		}
	}
}

func TestToolErrors(t *testing.T) {
	cfg, _ := analyzed(t, nil)
	cs := connect(t, cfg)
	for _, tc := range []struct {
		tool string
		args map[string]any
		want string
	}{
		{"usage", map[string]any{"fs": "nonexistent"}, "nonexistent"},
		{"usage", map[string]any{"prefix": "/elsewhere"}, "not within any of the configured filesystems"},
		{"usage", map[string]any{"prefix": "missing"}, "no such prefix"},
		{"children", map[string]any{"metric": "size"}, "storage-bytes"},
		{"top_n", map[string]any{"by": "owner"}, "subtree, prefix, user or group"},
		{"top_n", map[string]any{"by": "prefix", "prefix": "a", "user": "x"}, "only be set for by=prefix for the entire filesystem"},
		{"top_n", map[string]any{"by": "prefix", "user": "x", "group": "y"}, "user and group cannot both be set"},
	} {
		var out map[string]any
		if msg := call(t, cs, tc.tool, tc.args, &out); !strings.Contains(msg, tc.want) {
			t.Errorf("%v %v: got %q, want it to contain %q", tc.tool, tc.args, msg, tc.want)
		}
	}
}

func TestErrorsTool(t *testing.T) {
	if os.Geteuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("permissions cannot make a directory unreadable")
	}
	var locked string
	cfg, root := analyzed(t, func(root string) {
		locked = filepath.Join(root, "b", "locked")
		if err := os.Mkdir(locked, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	})
	cs := connect(t, cfg)
	for _, prefix := range []string{"", "b", filepath.Join(root, "b", "locked")} {
		var errs mcpserver.Errors
		mustCall(t, cs, "errors", map[string]any{"prefix": prefix}, &errs)
		if len(errs.Errors) != 1 || errs.Errors[0].Path != locked || !strings.Contains(errs.Errors[0].Message, "permission denied") {
			t.Errorf("%q: unexpected errors: %+v", prefix, errs)
		}
	}
	var errs mcpserver.Errors
	mustCall(t, cs, "errors", map[string]any{"prefix": "a"}, &errs)
	if len(errs.Errors) != 0 {
		t.Errorf("unexpected errors: %+v", errs)
	}
	mustCall(t, cs, "errors", map[string]any{"n": 0}, &errs)
	if len(errs.Errors) != 1 {
		t.Errorf("unexpected errors: %+v", errs)
	}
}

// TestLocked checks that the tools report that a database that is locked,
// eg. by an analysis, cannot be read, and work again once it is released.
func TestLocked(t *testing.T) {
	ctx := context.Background()
	cfg, _ := analyzed(t, nil)
	cs := connect(t, cfg, mcpserver.WithOpenTimeout(200*time.Millisecond))
	env := cli.NewEnv()
	env.OpenProgress = io.Discard
	db, err := env.OpenWritableDB(ctx, cfg.Filesystems[0].Config())
	if err != nil {
		t.Fatal(err)
	}
	var usage mcpserver.Usage
	if msg := call(t, cs, "usage", map[string]any{}, &usage); !strings.Contains(msg, "locked, probably because an analysis is running") {
		t.Errorf("unexpected error: %q", msg)
	}
	var fss mcpserver.Filesystems
	mustCall(t, cs, "list_filesystems", map[string]any{}, &fss)
	if len(fss.Filesystems) != 1 || !strings.Contains(fss.Filesystems[0].Status, "locked") {
		t.Errorf("unexpected filesystems: %+v", fss)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	mustCall(t, cs, "usage", map[string]any{}, &usage)
	if usage.Recursive == nil {
		t.Errorf("unexpected usage: %+v", usage)
	}
}

func Example() {
	ctx := context.Background()
	cfg, err := iduconfig.ParseConfig([]byte("filesystems:\n  - root: /data\n    database: /var/idu/data.db\n"))
	if err != nil {
		panic(err)
	}
	srv := mcpserver.New(cfg, mcpserver.WithOpenOptions(defaultdb.OpenOptions()...))
	// srv.Run(ctx, &mcp.StdioTransport{}) serves the tools on stdin/stdout.
	st, ct := mcp.NewInMemoryTransports()
	if _, err := srv.MCPServer().Connect(ctx, st, nil); err != nil {
		panic(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "example"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		panic(err)
	}
	defer cs.Close()
	for tool, err := range cs.Tools(ctx, nil) {
		if err != nil {
			panic(err)
		}
		fmt.Println(tool.Name)
	}
	// Output:
	// children
	// errors
	// list_filesystems
	// top_n
	// usage
}

// TestSelectFilesystem checks how the filesystem is chosen when several
// are configured.
func TestSelectFilesystem(t *testing.T) {
	cfg, root := analyzed(t, nil)
	tmpDir := t.TempDir()
	db := cfg.Filesystems[0].Config().Database
	local := filepath.Join(tmpDir, "local-db")
	multi := func(withLocal bool) iduconfig.T {
		t.Helper()
		spec := fmt.Sprintf("filesystems:\n  - name: tree\n    root: %s\n    database: %s\n  - name: other\n    root: %s\n    database: %s\n",
			root, db, filepath.Join(tmpDir, "other"), filepath.Join(tmpDir, "other-db"))
		if withLocal {
			spec += fmt.Sprintf("  - name: local\n    root: /\n    database: %s\n", local)
		}
		cfg, err := iduconfig.ParseConfig([]byte(spec))
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	cs := connect(t, multi(true), mcpserver.WithOpenTimeout(time.Second))

	// The local filesystem is the default.
	var fss mcpserver.Filesystems
	mustCall(t, cs, "list_filesystems", map[string]any{}, &fss)
	for _, f := range fss.Filesystems {
		if isDefault := f.Default != ""; isDefault != (f.Name == "local") {
			t.Errorf("%v: unexpected default: %q", f.Name, f.Default)
		}
	}
	var usage mcpserver.Usage
	if msg := call(t, cs, "usage", map[string]any{}, &usage); !strings.Contains(msg, "failed to open the database for local (/)") {
		t.Errorf("unexpected error: %q", msg)
	}
	// A filesystem is selected by its name, its root, or an absolute
	// prefix within it, the most specific root that contains the prefix
	// is used.
	for _, args := range []map[string]any{
		{"fs": "tree"},
		{"fs": root},
		{"prefix": root},
		{"prefix": filepath.Join(root, "a")},
		{"fs": "tree", "prefix": "a"},
	} {
		usage = mcpserver.Usage{}
		mustCall(t, cs, "usage", args, &usage)
		if !strings.HasPrefix(usage.Path, root) || usage.Recursive == nil {
			t.Errorf("%v: unexpected usage: %+v", args, usage)
		}
	}
	if msg := call(t, cs, "usage", map[string]any{"fs": "missing"}, &usage); !strings.Contains(msg, "configured filesystems: tree (") {
		t.Errorf("unexpected error: %q", msg)
	}
	// A prefix outside of all of the other roots is within the local
	// filesystem, on Windows / is not the root of the volume containing
	// tmpDir.
	if runtime.GOOS != "windows" {
		if msg := call(t, cs, "usage", map[string]any{"prefix": filepath.Join(tmpDir, "elsewhere")}, &usage); !strings.Contains(msg, "for local (/)") {
			t.Errorf("unexpected error: %q", msg)
		}
	}

	// The default can be configured.
	cs = connect(t, multi(true), mcpserver.WithDefaultFilesystem("tree"))
	usage = mcpserver.Usage{}
	mustCall(t, cs, "usage", map[string]any{"prefix": "a"}, &usage)
	if usage.Path != filepath.Join(root, "a") {
		t.Errorf("unexpected usage: %+v", usage)
	}

	// Without a local filesystem there is no default.
	cs = connect(t, multi(false))
	if msg := call(t, cs, "usage", map[string]any{}, &usage); !strings.Contains(msg, "none of them is the local filesystem") {
		t.Errorf("unexpected error: %q", msg)
	}
}
