// Copyright 2024 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Usage of idu
//
//	analyze disk usage using a database for incremental updates. Many of the commands
//	accept an expression that is used to restrict which prefixes/directories and
//	files are processed.
//
//	expression-syntax - display the syntax for the expression language supported by commands such as analyze, find etc.
//	          analyze - analyze the file system to build a database of directory and file metadata.
//	             logs - list the log of past operations stored in the database.
//	           errors - list the errors stored in the database
//	             find - find prefixes/files in the database that match the supplied expression.
//	            stats - compute and display statistics from the database.
//	          reports - generate and manage reports.
//	           config - describe the current configuration.
//	         database - database management commands.
//
// global flags: [--config=$HOME/.idu.yml --gcpercent=50 --http= --log-dir= --profile= --stderr=false --units=decimal --v=0]
//
//	-config string
//	  configuration file (default "$HOME/.idu.yml")
//	-gcpercent int
//	  value to use for runtime/debug.SetGCPercent (default 50)
//	-http string
//	  set to a port to enable http serving of /debug/vars and profiling
//	-log-dir string
//	  directory to write log files to
//	-profile value
//	  write a profile on exit; the format is <profile-name>:<file> and the
//	  flag may be repeated to request multiple profile types, use cpu to request
//	  cpu profiling in addition to predefined profiles in runtime/pprof
//	-stderr
//	  write log messages to stderr
//	-units string
//	  display usage in decimal (KB) or binary (KiB) formats (default "decimal")
//	-v int
//	  lower values show more debugging output
package main
