// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"

	// G108
	_ "net/http/pprof" //nolint:gosec
	"runtime"
	debugpkg "runtime/debug"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/cmdutil/profiling"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/file/diskusage"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	globalFlags  GlobalFlags
	globalConfig config.T
)

var (
	panicBuf     = make([]byte, 1024*1024)
	bytesPrinter func(size int64) (float64, string)
)

var commands = `name: idu
summary: analyze disk usage using a database for incremental updates
commands:
  - name: analyze
    summary: analyze the file system to build a database of file counts, disk usage etc
    arguments:
      - <prefix>

  - name: logs
    summary: list the log of past operations stored in the database
    arguments:
      - <prefix>

  - name: errors
    summary: list the errors stored in the database
    arguments:
      - <prefix>

  - name: find
    summary: find files matching the specified criteria
    arguments:
     - <prefix>

  - name: stats
    summary: compute and display statistics from the database
    commands:
      - name: compute
        summary: compute all statistics
        arguments:
          - <prefix>
      - name: aggregate
        summary: display aggregated/tatal stats
        arguments:
          - <prefix>
      - name: user
        summary: summarize file count and disk usage on a per user basis
        arguments:
          - <prefix>
          - '[user]...'
      - name: group
        summary: summarize file count and disk usage on a per group basis
        arguments:
          - <prefix>
          - '[group]...'
      - name: list
        summary: list the available stats
        arguments:
          - <prefix>
      - name: reports
        summary: generate reports in a variety of formats, including tsv, json and markdown
        arguments:
          - <prefix>
      - name: erase
        summary: erase the stats
        arguments:
          - <prefix>

  - name: ls
    summary: list the contents of the database
    arguments:
       - <prefix>
       - '[prefixes]...'

  - name: config
    summary: describe the current configuration

  - name: du
    summary: display disk usage for the specified directory tree without using a database
    arguments:
      - <directory>

  - name: database
    summary: database management commands
    commands:
    - name: locate
      summary: display the location of the database
      arguments:
        - <prefix>
`

type GlobalFlags struct {
	ExitProfile profiling.ProfileFlag `subcmd:"profile,,'write a profile on exit; the format is <profile-name>:<file> and the flag may be repeated to request multiple profile types, use cpu to request cpu profiling in addition to predefined profiles in runtime/pprof'"`
	ConfigFile  string                `subcmd:"config,$HOME/.idu.yml,configuration file"`
	Units       string                `subcmd:"units,decimal,display usage in decimal (KB) or binary (KiB) formats"`
	Verbose     int                   `subcmd:"v,0,lower values show more debugging output"`
	LogDir      string                `subcmd:"log-dir,,directory to write log files to"`
	Stderr      bool                  `subcmd:"stderr,false,write log messages to stderr"`
	HTTP        string                `subcmd:"http,,set to a port to enable http serving of /debug/vars and profiling"`
	GCPercent   int                   `subcmd:"gcpercent,50,value to use for runtime/debug.SetGCPercent"`
	UseBadgerDB bool                  `subcmd:"use-badger-db,true,use badgerdb instead of boltdb"`
	UseBoltDB   bool                  `subcmd:"use-bolt-db,false,use boltdb instead of badgerdb"`
}

func cli() *subcmd.CommandSetYAML {
	cmdSet := subcmd.MustFromYAML(commands)

	analyzer := &analyzeCmd{}
	cmdSet.Set("analyze").MustRunner(analyzer.analyze, &analyzeFlags{})

	ls := &lister{}
	cmdSet.Set("ls").MustRunner(ls.prefixes, &lsFlags{})
	cmdSet.Set("errors").MustRunner(ls.errors, &errorFlags{})
	cmdSet.Set("logs").MustRunner(ls.logs, &logFlags{})

	statsCmd := &statsCmds{}
	cmdSet.Set("stats", "compute").MustRunner(statsCmd.compute, &computeFlags{})
	cmdSet.Set("stats", "aggregate").MustRunner(statsCmd.aggregate, &aggregateFlags{})
	cmdSet.Set("stats", "user").MustRunner(statsCmd.user, &userFlags{})
	cmdSet.Set("stats", "group").MustRunner(statsCmd.group, &groupFlags{})
	cmdSet.Set("stats", "list").MustRunner(statsCmd.list, &listStatsFlags{})
	cmdSet.Set("stats", "erase").MustRunner(statsCmd.erase, &eraseFlags{})
	cmdSet.Set("stats", "reports").MustRunner(statsCmd.reports, &reportsFlags{})

	findCmds := &findCmds{}
	cmdSet.Set("find").MustRunner(findCmds.find, &findFlags{})

	duCmd := &duCmd{}
	cmdSet.Set("du").MustRunner(duCmd.du, &duFlags{})

	cmdSet.Set("config").MustRunner(configManager, &configFlags{})

	db := &dbCmd{}
	cmdSet.Set("database", "locate").MustRunner(db.locate, &locateFlags{})

	globals := subcmd.GlobalFlagSet()
	globals.MustRegisterFlagStruct(&globalFlags, nil, nil)
	cmdSet.WithGlobalFlags(globals)
	cmdSet.WithMain(mainWrapper)
	return cmdSet
}

func mainWrapper(ctx context.Context, cmdRunner func(ctx context.Context) error) error {

	debugpkg.SetGCPercent(globalFlags.GCPercent)

	err := flags.OneOf(globalFlags.Units).Validate("decimal", "decimal", "binary")
	if err != nil {
		return err
	}
	switch globalFlags.Units {
	case "decimal":
		bytesPrinter = func(size int64) (float64, string) {
			return diskusage.DecimalBytes(size).Standardize()
		}
	case "binary":
		bytesPrinter = func(size int64) (float64, string) {
			return diskusage.Base2Bytes(size).Standardize()
		}
	}
	for _, profile := range globalFlags.ExitProfile.Profiles {
		if !profiling.IsPredefined(profile.Name) {
			fmt.Printf("warning profile %v defaults to CPU profiling since it is not one of the predefined profile types: %v", profile.Name, strings.Join(profiling.PredefinedProfiles(), ", "))
		}
		save, err := profiling.Start(profile.Name, profile.Filename)
		if err != nil {
			return err
		}
		fmt.Printf("profiling: %v %v\n", profile.Name, profile.Filename)
		defer save()
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic: %v\n", r)
			runtime.Stack(panicBuf, true)
			fmt.Println(string(panicBuf))
		}
	}()

	cfg, err := config.ReadConfig(globalFlags.ConfigFile)
	if err != nil {
		return err
	}
	globalConfig = cfg

	var ln net.Listener
	if port := globalFlags.HTTP; len(port) > 0 {
		if ln, err = net.Listen("tcp", port); err != nil {
			return err
		}
		// gosec G114
		go http.Serve(ln, nil) //nolint:gosec
	}

	db := 0
	if globalFlags.UseBadgerDB {
		internal.UseBadgerDB()
		db++
	}
	if globalFlags.UseBoltDB {
		internal.UseBoltDB()
		db++
	}
	if db != 1 {
		return fmt.Errorf("must specify exactly one of use-badger-db or use-bolt-db")
	}

	internal.Verbosity = slog.Level(globalFlags.Verbose)
	internal.LogDir = globalFlags.LogDir
	if internal.LogDir == "" {
		internal.LogDir = os.TempDir()
	}

	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt, os.Kill)
	defer cancel()
	return cmdRunner(ctx)
}

func main() {
	cli().MustDispatch(context.Background())
}

var printer = message.NewPrinter(language.English)

func fmtSize(size int64) string {
	f, u := bytesPrinter(size)
	return printer.Sprintf("% 8.3f %s", f, u)
}

func fmtCount(count int64) string {
	return printer.Sprintf("% 11v", count)
}

func banner(out io.Writer, ul string, format string, args ...any) {
	buf := strings.Builder{}
	o := fmt.Sprintf(format, args...)
	buf.WriteString(o)
	buf.WriteString(strings.Repeat(ul, len(o)))
	buf.WriteRune('\n')
	out.Write([]byte(buf.String()))
}
