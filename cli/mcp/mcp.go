// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package mcp provides the mcp command, which runs the Model Context
// Protocol server implemented by mcpserver on stdin/stdout so that AI
// agents can query the databases of the configured filesystems. For
// example, to make it available to Claude Code:
//
//	claude mcp add idu -- idu mcp
//
// or, for a specific configuration file:
//
//	claude mcp add idu -- idu --config=<file> mcp
package mcp

import (
	"context"
	"time"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/mcpserver"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Flags are the flags for the mcp command.
type Flags struct {
	OpenTimeout time.Duration `subcmd:"open-timeout,5s,how long a tool waits for a database that is locked by an analysis before reporting that it is"`
}

// Commands implements the mcp command.
type Commands struct {
	env       *cli.Env
	transport func() sdkmcp.Transport
}

// New returns the Commands for env.
func New(env *cli.Env) *Commands {
	return &Commands{env: env, transport: func() sdkmcp.Transport { return &sdkmcp.StdioTransport{} }}
}

// MCP implements the mcp command, values must be a *Flags.
func (c *Commands) MCP(ctx context.Context, values any, _ []string) error {
	fl := values.(*Flags)
	// Messages written while waiting for a locked database are discarded
	// by the server since stdout is used by the stdio transport.
	srv := mcpserver.New(c.env.Config,
		mcpserver.WithOpenOptions(idudb.WithBackendFactory(c.env.DatabaseFactory)),
		mcpserver.WithOpenTimeout(fl.OpenTimeout),
		mcpserver.WithDefaultFilesystem(c.env.FS))
	return srv.Run(ctx, c.transport())
}
