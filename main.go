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
	"syscall"

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
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
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
summary: analyze disk usage using a database for incremental updates. Many of the commands accept an expression that is used to restrict which prefixes/directories and files are processed.
commands:
  - name: expression-syntax
    summary: display the syntax for the expression language supported by commands such as analyze, find etc.

  - name: analyze
    summary: analyze the file system to build a database of directory and file metadata.
    arguments:
      - <prefix>

  - name: logs
    summary: list the log of past operations stored in the database.
    arguments:
      - <prefix>

  - name: errors
    summary: list the errors stored in the database
    arguments:
      - <prefix>

  - name: find
    summary: find prefixes/files in the database that match the supplied expression.
    arguments:
     - <prefix>
     - <expression>...

  - name: stats
    summary: compute and display statistics from the database.
    commands:
      - name: compute
        summary: compute all statistics based on the current state of the database and
                 save the results to a file. The results can be viewed using the
				 stats view command.
        arguments:
          - <prefix>
          - <expression>...

      - name: view
        summary: view the statistics stored in a file created using the compute command.
        arguments:
          - <filename>


  - name: reports
    summary: generate and manage reports.
    commands:
      - name: generate
        summary:  generate reports in a variety of formats, including tsv, json and markdown from the statistics stored in the specified file.
        arguments:
          - <filename>
          - ...

      - name: locate
        summary: locate the last n sets of reports in a given directory as a json array of filenames. This is intended to be used by other scripts that analyze the reports.
        arguments:
          - <report-directory>

  - name: config
    summary: describe the current configuration.

  - name: database
    summary: database management commands.
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
}

func init() {
	config.DefaultConcurrentStats = asyncstat.DefaultAsyncStats
	config.DefaultConcurrentStatsThreshold = asyncstat.DefaultAsyncThreshold
	config.DefaultConcurrentScans = filewalk.DefaultConcurrentScans
	config.DefaultScanSize = filewalk.DefaultScanSize
}

func cli() *subcmd.CommandSetYAML {
	cmdSet := subcmd.MustFromYAML(commands)

	expr := &exprCmd{}
	cmdSet.Set("expression-syntax").MustRunner(expr.explain, &struct{}{})

	analyzer := &analyzeCmd{}
	cmdSet.Set("analyze").MustRunner(analyzer.analyze, &analyzeFlags{})

	ls := &lister{}
	cmdSet.Set("errors").MustRunner(ls.errors, &errorFlags{})
	cmdSet.Set("logs").MustRunner(ls.logs, &logFlags{})

	statsCmd := &statsCmds{}
	cmdSet.Set("stats", "compute").MustRunner(statsCmd.compute, &computeFlags{})

	cmdSet.Set("stats", "view").MustRunner(statsCmd.view, &viewFlags{})

	reportsCmds := &reportCmds{}
	cmdSet.Set("reports", "generate").MustRunner(reportsCmds.generate, &generateReportsFlags{})
	cmdSet.Set("reports", "locate").MustRunner(reportsCmds.locate, &locateReportsFlags{})

	findCmds := &findCmds{}
	cmdSet.Set("find").MustRunner(findCmds.find, &findFlags{})

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

	internal.UseBadgerDB()

	internal.Verbosity = slog.Level(globalFlags.Verbose)
	internal.LogDir = globalFlags.LogDir
	if internal.LogDir == "" {
		internal.LogDir = os.TempDir()
	}

	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt, syscall.SIGTERM)
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
