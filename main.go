// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	// G108
	_ "net/http/pprof" //nolint:gosec
	"path/filepath"
	"runtime"
	debugpkg "runtime/debug"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/cmdutil/profiling"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/file/diskusage"
	"golang.org/x/exp/slog"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	globalFlags  GlobalFlags
	globalConfig config.T
	globalLogger *slog.Logger
)

var (
	panicBuf     = make([]byte, 1024*1024)
	bytesPrinter func(size int64) (float64, string)
)

var commands = `name: idu
summary: determine disk usage incrementally using a database
commands:
  - name: analyze
    summary: analyze the file system to build a database of file counts, disk usage etc
    arguments:
      - <prefix>
  - name: logs
    summary: list the log of past operations in the database
    arguments:
      - <prefix>
  - name: errors
    summary: list the errors in the database
    arguments:
      - <prefix>
  - name: summary
    summary: summarize file count and disk usage
    arguments:
      - <prefix>
  - name: user
    summary: summarize file count and disk usage on a per user basis
  - name: group
    summary: summarize file count and disk usage on a per group basis
  #- name: find
  #  summary: find prefixes/files in statistics database
  - name: ls
    summary: list the contents of the database
    arguments:
       - <prefix>

  - name: config
    summary: describe the current configuration
   
  - name: database
    summary: database management commands
    commands:
    - name: stats
      summary: display database stastistics
    - name: erase
      summary: erase the database
    - name: compact
      summary: compact the database
`

type GlobalFlags struct {
	ExitProfile profiling.ProfileFlag `subcmd:"profile,,'write a profile on exit; the format is <profile-name>:<file> and the flag may be repeated to request multiple profile types, use cpu to request cpu profiling in addition to predefined profiles in runtime/pprof'"`
	Human       bool                  `subcmd:"h,true,show sizes in human readable form"`
	ConfigFile  string                `subcmd:"config,$HOME/.idu.yml,configuration file"`
	Units       string                `subcmd:"units,decimal,display usage in decimal (KB) or binary (KiB) formats"`
	Verbose     int                   `subcmd:"v,0,higher values show more debugging output"`
	Log         string                `subcmd:"log,,'logfile to write to, if empty then stderr is used'"`
	HTTP        string                `subcmd:"http,,set to a port to enable http serving of /debug/vars and profiling"`
	GCPercent   int                   `subcmd:"gcpercent,50,value to use for runtime/debug.SetGCPercent"`
}

func cli() *subcmd.CommandSetYAML {
	cmdSet := subcmd.MustFromYAML(commands)

	analyzer := &analyzeCmd{}
	cmdSet.Set("analyze").MustRunner(analyzer.analyze, &analyzeFlags{})

	ls := &lister{}
	cmdSet.Set("ls").MustRunner(ls.prefixes, &lsFlags{})
	cmdSet.Set("errors").MustRunner(ls.errors, &errorFlags{})
	cmdSet.Set("logs").MustRunner(ls.logs, &logFlags{})

	cmdSet.Set("summary").MustRunner(nil, &summaryFlags{})
	cmdSet.Set("user").MustRunner(nil, &userFlags{})
	cmdSet.Set("group").MustRunner(nil, &groupFlags{})
	//	cmdSet.Set("find").MustRunner(nil, &findFlags{})

	cmdSet.Set("config").MustRunner(configManager, &configFlags{})
	// db := &database{}
	//	cmdSet.Set("database", "stats").MustRunner(db.stats, &struct{}{})
	//	cmdSet.Set("database", "erase").MustRunner(db.erase, &eraseFlags{})
	//cmdSet.Set("database", "compact").MustRunner(db.compact, &compactFlags{})
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

	internal.Verbosity = internal.LogLevel(globalFlags.Verbose)
	var logFile = os.Stderr
	if globalFlags.Log != "" {
		logFile, err = os.Create(globalFlags.Log)
		if err != nil {
			return err
		}
	}
	globalLogger = slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{}))

	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt, os.Kill)
	defer cancel()
	return cmdRunner(ctx)
}

func main() {
	cli().MustDispatch(context.Background())
}

func debug(ctx context.Context, level int, format string, args ...interface{}) {
	if level > globalFlags.Verbose {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("%s: %s:% 4d: ", time.Now().Format(time.RFC3339), filepath.Base(file), line)
	fmt.Printf(format, args...)
}

var printer = message.NewPrinter(language.English)

func fmtSize(size int64) string {
	if globalFlags.Human {
		f, u := bytesPrinter(size)
		return printer.Sprintf("%0.3f %s", f, u)
	}
	return printer.Sprintf("%v", size)
}
