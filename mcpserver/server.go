// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package mcpserver provides a Model Context Protocol (MCP) server that
// allows AI agents to query the databases of the filesystems analyzed by
// idu: the filesystems that are configured, the totals for any prefix and
// its children, the top-N prefixes, subtrees, users and groups by any
// metric, and the errors recorded by analyze. All of its tools are
// read-only.
//
// The server uses only idu's public packages and may be embedded in other
// commands, it is run by idu mcp. For example, to serve the configured
// filesystems on stdin/stdout:
//
//	cfg, _, err := iduconfig.Load(ctx, &cmdconfig.FilesFlag{})
//	srv := mcpserver.New(cfg, mcpserver.WithOpenOptions(defaultdb.OpenOptions()...))
//	err = srv.Run(ctx, &mcp.StdioTransport{})
//
// Each tool call opens the database it needs, for reading only, and closes
// it before returning so that the server never prevents analyze from
// opening the database for writing. Conversely, while an analysis is
// running, its database is locked and the tools that need it fail, after
// a timeout, see WithOpenTimeout, with an error that says so.
package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/summary"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// DefaultOpenTimeout is the default time that a tool waits for a database
// that is locked, eg. by an analysis, to be released.
const DefaultOpenTimeout = 5 * time.Second

// ErrLocked is returned, wrapped, by tools whose database could not be
// opened within the open timeout, typically because an analysis is running.
var ErrLocked = errors.New("the database is locked, probably because an analysis is running: try again later")

// Option configures a Server.
type Option func(*options)

type options struct {
	openOptions []idudb.OpenOption
	openTimeout time.Duration
	version     string
	defaultFS   string
}

// WithOpenOptions sets the options used to open databases, which must
// include idudb.WithBackendFactory, see defaultdb.OpenOptions. Any messages
// written while waiting for a locked database are discarded unless
// idudb.WithOpenProgress is included, which must not write to the
// transport, eg. stdout for the stdio transport.
func WithOpenOptions(opts ...idudb.OpenOption) Option {
	return func(o *options) {
		o.openOptions = append(o.openOptions, opts...)
	}
}

// WithOpenTimeout sets the time that a tool waits for a locked database to
// be released, DefaultOpenTimeout by default.
func WithOpenTimeout(d time.Duration) Option {
	return func(o *options) {
		o.openTimeout = d
	}
}

// WithDefaultFilesystem sets the filesystem, by name or location, used by
// tool calls that specify neither a filesystem nor an absolute prefix. By
// default it is the only filesystem configured or, if there are several,
// the local one, ie. the one whose root is /.
func WithDefaultFilesystem(fs string) Option {
	return func(o *options) {
		o.defaultFS = fs
	}
}

// WithVersion sets the version reported by the server.
func WithVersion(v string) Option {
	return func(o *options) {
		o.version = v
	}
}

// Server is an MCP server for the filesystems in an idu configuration.
type Server struct {
	cfg  iduconfig.T
	opts options
	srv  *mcp.Server

	mu sync.Mutex
	// dbLocks serialize the use of each database, keyed by its location,
	// since a database must not be opened more than once by a process.
	dbLocks map[string]*sync.Mutex
}

// New returns a Server for the filesystems in cfg.
func New(cfg iduconfig.T, opts ...Option) *Server {
	s := &Server{
		cfg:     cfg,
		opts:    options{openTimeout: DefaultOpenTimeout},
		dbLocks: map[string]*sync.Mutex{},
	}
	// Discard any messages written while waiting for a locked database
	// unless the caller provides somewhere else to write them.
	s.opts.openOptions = []idudb.OpenOption{idudb.WithOpenProgress(io.Discard)}
	for _, fn := range opts {
		fn(&s.opts)
	}
	s.srv = mcp.NewServer(&mcp.Implementation{
		Name:    "idu",
		Title:   "idu disk usage",
		Version: s.opts.version,
	}, &mcp.ServerOptions{Instructions: instructions})
	s.addTools()
	return s
}

const instructions = `idu analyzes filesystems, recording the metadata of every directory (prefix) and file in a database, and maintains totals and top-N rankings that these tools query. Start with list_filesystems to find the configured filesystems and when each was last analyzed. usage, children and top_n answer the common questions, query provides the full row model, including hotspots, ie. prefixes whose subtrees accumulate substantial usage spread across many descendants. A request that would require a pass over the database fails with an error that explains what is supported instead. Prefixes are paths, either absolute or relative to the filesystem's root; omit the prefix for the entire filesystem. When fs is omitted, an absolute prefix selects the filesystem that contains it and otherwise the default filesystem, marked as such by list_filesystems, is used. Sizes are in bytes: bytes is the total size of files and storage_bytes the space they occupy on disk, which is usually the more useful measure of disk usage. Files with more than one link are counted once. The data is only as current as the most recent analysis.`

// MCPServer returns the underlying mcp.Server, eg. to add further tools or
// to connect it to a transport.
func (s *Server) MCPServer() *mcp.Server {
	return s.srv
}

// Run runs the server on transport until the client disconnects or ctx is
// canceled.
func (s *Server) Run(ctx context.Context, transport mcp.Transport) error {
	return s.srv.Run(ctx, transport)
}

func (s *Server) dbLock(f iduconfig.Filesystem) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := f.Config().Database
	l, ok := s.dbLocks[key]
	if !ok {
		l = &sync.Mutex{}
		s.dbLocks[key] = l
	}
	return l
}

// withDB opens the database for f, for reading only, calls fn with it and
// closes it. It returns an error that wraps ErrLocked if the database
// cannot be opened within the open timeout.
func (s *Server) withDB(ctx context.Context, f iduconfig.Filesystem, fn func(db idudb.Reader, sr *summary.Reader) error) error {
	l := s.dbLock(f)
	l.Lock()
	defer l.Unlock()
	cfg := f.Config()
	openCtx, cancel := context.WithTimeout(ctx, s.opts.openTimeout)
	defer cancel()
	db, err := idudb.Open(openCtx, cfg, s.opts.openOptions...)
	if err != nil {
		if ctx.Err() == nil && errors.Is(openCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%v: %w", iduconfig.Label(f), ErrLocked)
		}
		return fmt.Errorf("failed to open the database for %v: %w", iduconfig.Label(f), err)
	}
	defer db.Close(context.WithoutCancel(ctx))
	return fn(db, summary.New(db, cfg))
}
