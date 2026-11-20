// Copyright 2024 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Usage of idu
//
//	analyze disk usage using a database for incremental updates. The filesystem to
//	operate on is selected by name using the --fs flag, which every command that
//	operates on a filesystem accepts, and which may also be given as a global flag
//	that the command flag overrides; it may be omitted if only one filesystem is
//	configured. Commands that accept a <prefix> interpret it relative to that
//	filesystem's configured root, or as a key prefix for a filesystem configured as
//	a key range, and it must be contained within that root or range; an omitted
//	[<prefix>] refers to the entire filesystem. Many of the commands accept an
//	expression that is used to restrict which prefixes/directories and files are
//	processed.
//
//	expression-syntax - display the syntax for the expression language supported by commands such as analyze, find etc.
//	          analyze - analyze the file system to build a database of directory and file metadata.
//	             logs - list the log of past operations stored in the database.
//	           errors - list the errors stored in the database
//	             find - find prefixes/files in the database that match the supplied expression.
//	            stats - compute and display statistics from the database.
//	          reports - generate and manage reports.
//	           config - show the configuration, and its documentation.
//	         database - database management commands.
//
// global flags: [--config= --fs= --gcpercent=50 --http= --log-file= --log-format=json --log-level=0 --log-source-code=false --log-tempfile= --profile= --units=decimal]
//
//	-config value
//	  configuration files (.yaml, .yml, .env) or comma-separated list; defaults
//	  to config.yaml (and its filesystem_config_files) in the idu search path
//	  or all .yaml/.yml files in $HOME/.config/idu
//	-fs string
//	  the name of the configured filesystem to operate on, it may be omitted
//	  if only one filesystem is configured. For a filesystem configured with
//	  a root, any prefix arguments are interpreted relative to, and must be
//	  contained within, that root; for one configured with a key range they
//	  are key prefixes that must be contained within that range
//	-gcpercent int
//	  value to use for runtime/debug.SetGCPercent (default 50)
//	-http string
//	  set to a port to enable http serving of /debug/vars and profiling
//	-log-file string
//	  log file path. If not specified logs are written to stderr, if set to -
//	  logs are written to stdout
//	-log-format string
//	  log format: text or json (default "json")
//	-log-level int
//	  logging level: 0=error, 1=warn, 2=info, 3=debug
//	-log-source-code
//	  include source code file and line number in logs
//	-log-tempfile string
//	  create a unique log file from this os.CreateTemp pattern, eg. myapp-*.log,
//	  in os.TempDir unless the pattern includes a directory, which is created
//	  if needed. Mutually exclusive with log-file
//	-profile value
//	  write a profile on exit; the format is <profile-name>:<file> and the
//	  flag may be repeated to request multiple profile types, use cpu to request
//	  cpu profiling in addition to predefined profiles in runtime/pprof
//	-units value
//	  display usage in decimal (KB) or binary (KiB) formats
package main
