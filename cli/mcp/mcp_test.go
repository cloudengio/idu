// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"io"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestMCP checks that the mcp command serves the configured filesystems.
func TestMCP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tmpDir, root := clitest.NewHardlinkTree(t)
	env := cli.NewEnv()
	env.Config = clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	env.OpenProgress = io.Discard
	if err := analyze.New(env, analyze.WithOutput(io.Discard)).Analyze(ctx, &analyze.Flags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
	st, ct := sdkmcp.NewInMemoryTransports()
	c := New(env)
	c.transport = func() sdkmcp.Transport { return st }
	done := make(chan error, 1)
	go func() { done <- c.MCP(ctx, &Flags{OpenTimeout: time.Second}, nil) }()

	cs, err := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{Name: "usage", Arguments: map[string]any{}})
	if err != nil || res.IsError {
		t.Fatalf("usage: %v %+v", err, res)
	}
	sc, ok := res.StructuredContent.(map[string]any)
	if !ok || sc["path"] != root {
		t.Errorf("unexpected result: %+v", res.StructuredContent)
	}
	cs.Close()
	select {
	case err := <-done:
		t.Logf("server exited: %v", err)
	case <-time.After(10 * time.Second):
		t.Errorf("the server did not exit when the client disconnected")
	}
}
