// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Usage of idu
//
//  idu: analyze file systems to create a database of per-file and aggregate size
//  stastistics to support incremental updates and subsequent interrogation. Local
//  and cloud based filesystems are contemplated. See https://github.com/cloudengio/blob/master/idu/README.md
//  for full details.
//
//         analyze - analyze the file system to build a database of file counts, disk usage etc
//          config - describe the current configuration
//  erase-database - erase the file statistics database
//          errors - list the contents of the errors database
//             lsr - list the contents of the database
//           query - query the file statistics database
//         summary - summarize file count and disk usage
//            user - summarize file count and disk usage on a per user basis
//           group - summarize file count and disk usage on a per group basis
//   refresh-stats - refresh statistics by recalculating them over the entire database
//
// global flags: [--config=$HOME/.idu.yml --exit-profile= --h=true --units=decimal --v=0]
//   -config string
//     configuration file (default "$HOME/.idu.yml")
//   -exit-profile value
//     write a profile on exit; the format is <profile-name>:<file> and the
//     flag may be repeated to request multiple profile types, use cpu to request
//     cpu profiling in addition to predefined profiles in runtime/pprof
//   -h	show sizes in human readable form (default true)
//   -units string
//     display usage in decimal (KB) or binary (KiB) formats (default "decimal")
//   -v int
//     higher values show more debugging output
//
// flag: help requested
package main
