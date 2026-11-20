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
	"strings"
	"syscall"

	// G108
	_ "net/http/pprof" //nolint:gosec
	"runtime"
	debugpkg "runtime/debug"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/cli/find"
	"cloudeng.io/cmd/idu/cli/reports"
	"cloudeng.io/cmd/idu/cli/stats"
	"cloudeng.io/cmd/idu/cli/tui"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/cmdconfig"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/cmdutil/profiling"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
)

var (
	globalFlags GlobalFlags
	// env is the environment shared by all of the commands, its Config
	// and FS are set by mainWrapper.
	env = cli.NewEnv()
)

var panicBuf = make([]byte, 1024*1024)

var commands = `name: idu
summary: analyze disk usage using a database for incremental updates. The filesystem to operate on is selected by name using the --fs flag, which every command that operates on a filesystem accepts, and which may also be given as a global flag that the command flag overrides; it may be omitted if only one filesystem is configured. Commands that accept a <prefix> interpret it relative to that filesystem's configured root, or as a key prefix for a filesystem configured as a key range, and it must be contained within that root or range; an omitted [<prefix>] refers to the entire filesystem. Many of the commands accept an expression that is used to restrict which prefixes/directories and files are processed.
commands:
  - name: expression-syntax
    summary: display the syntax for the expression language supported by commands such as analyze, find etc.

  - name: analyze
    summary: analyze the file system to build a database of directory and file metadata.
    arguments:
      - '[<prefix>]'

  - name: logs
    summary: list the log of past operations stored in the database.
    arguments:
      - '[<prefix>]'

  - name: errors
    summary: list the errors stored in the database
    arguments:
      - '[<prefix>]'

  - name: tui
    summary: an interactive, terminal based, view of the filesystem specified by --fs that displays its prefixes as a tree together with the summary of, and the top-N rankings beneath, the highlighted prefix, and can run analyze, see tui-design.md. The optional prefix is highlighted initially.
    arguments:
      - '[<prefix>]'

  - name: find
    summary: find prefixes/files in the database that match the supplied expression.
    arguments:
     - <prefix>
     - <expression>...

  - name: stats
    summary: compute and display statistics from the database.
    commands:
      - name: compute
        summary: compute statistics for a prefix, optionally restricted by an expression, from the current state of the database and save them as a snapshot in snapshot_dir, or --snapshot-dir, or to the file specified by --stats-file. The results can be viewed using the stats view command.
        arguments:
          - <prefix>
          - <expression>...

      - name: view
        summary: view the current statistics for the filesystem specified by --fs, ie. the most recent snapshot in its snapshot_dir that is more recent than the last update to its database or, if there is none, statistics constructed from the database; a new snapshot is written to snapshot_dir, if set, when using the database requires a full pass over it. If a filename is specified that snapshot is displayed instead, a relative filename that does not exist is looked for in the snapshot directory, which is specified by --snapshot-dir or is the snapshot_dir of the filesystem specified by --fs.
        arguments:
          - '[<filename>]'

      - name: suggest
        summary: suggest configuration settings based on the state of the database. It currently suggests the prefixes to add to hardlinks_record_all, based on the files found by analyze to have more links than it has seen; such files are only found when hardlink_dedup is summaries or summaries-all and topn is not zero. An optional prefix restricts the suggestions to the prefixes beneath it.
        arguments:
          - '[<prefix>]'


  - name: reports
    summary: generate and manage reports.
    commands:
      - name: generate
        summary: generate reports in a variety of formats, including tsv, json and markdown, from the specified snapshot files or, if none are specified, from the current statistics for the filesystem specified by --fs, ie. the most recent snapshot in its snapshot_dir that is more recent than the last update to its database, or the database itself; a new snapshot is written to snapshot_dir, if set, when using the database requires a full pass over it.
        arguments:
          - '[<filename>]...'

      - name: locate
        summary: locate the last n sets of reports in a given directory, or the reports_dir of the filesystem specified by --fs if none is given, as a json array of filenames. This is intended to be used by other scripts that analyze the reports.
        arguments:
          - '[<report-directory>]'

  {{range subcmdExtension "config"}}{{.}}
  {{end}}

  - name: database
    summary: database management commands.
    commands:
    - name: locate
      summary: display the location of the database
      arguments:
        - '[<prefix>]'
`

type units int

const (
	unitsDecimal units = iota
	unitsBinary
)

func (u units) EnumValues() map[string]units {
	return map[string]units{
		"decimal": unitsDecimal,
		"binary":  unitsBinary,
	}
}

type GlobalFlags struct {
	cmdutil.LoggingFlags
	FS          string                `subcmd:"fs,,'the name of the configured filesystem to operate on, it may be omitted if only one filesystem is configured. For a filesystem configured with a root, any prefix arguments are interpreted relative to, and must be contained within, that root; for one configured with a key range they are key prefixes that must be contained within that range'"`
	ExitProfile profiling.ProfileFlag `subcmd:"profile,,'write a profile on exit; the format is <profile-name>:<file> and the flag may be repeated to request multiple profile types, use cpu to request cpu profiling in addition to predefined profiles in runtime/pprof'"`
	Config      cmdconfig.FilesFlag   `subcmd:"config,,'configuration files (.yaml, .yml, .env) or comma-separated list; defaults to config.yaml (and its filesystem_config_files) in the idu search path or all .yaml/.yml files in $HOME/.config/idu'"`
	Units       flags.Enum[units]     `subcmd:"units,decimal,display usage in decimal (KB) or binary (KiB) formats"`
	HTTP        string                `subcmd:"http,,set to a port to enable http serving of /debug/vars and profiling"`
	GCPercent   int                   `subcmd:"gcpercent,50,value to use for runtime/debug.SetGCPercent"`
}

func init() {
	iduconfig.DefaultConcurrentStats = asyncstat.DefaultAsyncStats
	iduconfig.DefaultConcurrentStatsThreshold = asyncstat.DefaultAsyncThreshold
	iduconfig.DefaultConcurrentScans = filewalk.DefaultConcurrentScans
	iduconfig.DefaultScanSize = filewalk.DefaultScanSize
}

func requireConfig(runner subcmd.Runner) subcmd.Runner {
	return func(ctx context.Context, flagValues any, args []string) error {
		if loadErr := cli.ConfigLoadError(ctx); loadErr != nil {
			return loadErr
		}
		return runner(ctx, flagValues, args)
	}
}

// squashErrors returns a runner that squashes the, potentially thousands
// of, context.Canceled and context.DeadlineExceeded errors returned by
// runner when it is interrupted, see cli.SquashCanceled.
// logsToFile returns true if log output is written to a file, rather than
// the terminal, as specified by the --log-* flags or the configuration.
func logsToFile() bool {
	toFile := func(file, temp string) bool {
		return temp != "" || (file != "" && file != "-")
	}
	if toFile(globalFlags.File, globalFlags.TempFile) {
		return true
	}
	return toFile(env.Config.Logging.File, env.Config.Logging.TempFile)
}

func squashErrors(runner subcmd.Runner) subcmd.Runner {
	return func(ctx context.Context, flagValues any, args []string) error {
		return cli.SquashCanceled(runner(ctx, flagValues, args))
	}
}

func newCommandSet() *subcmd.CommandSetYAML {
	cmdSet := subcmd.MustFromYAMLTemplate(commands, newConfigExtension())
	cmdSet.MustAddExtensions()
	cmdSet.Set("config", "show").MustRunner(squashErrors(configShow), &struct{}{})
	cmdSet.Set("config", "describe").MustRunner(squashErrors(configDescribe), &struct{}{})

	expr := &exprCmd{}
	cmdSet.Set("expression-syntax").MustRunner(squashErrors(expr.explain), &struct{}{})

	st := stats.New(env)
	alz := analyze.New(env, analyze.WithProgress(newAnalyzeProgress), analyze.WithSnapshot(st.Snapshot))
	cmdSet.Set("analyze").MustRunner(squashErrors(requireConfig(alz.Analyze)), &analyze.Flags{})
	cmdSet.Set("errors").MustRunner(squashErrors(requireConfig(alz.Errors)), &analyze.ErrorFlags{})
	cmdSet.Set("logs").MustRunner(squashErrors(requireConfig(alz.Logs)), &analyze.LogFlags{})
	cmdSet.Set("database", "locate").MustRunner(squashErrors(requireConfig(alz.Locate)), &analyze.LocateFlags{})

	cmdSet.Set("stats", "compute").MustRunner(squashErrors(requireConfig(st.Compute)), &stats.ComputeFlags{})
	cmdSet.Set("stats", "view").MustRunner(squashErrors(st.View), &stats.ViewFlags{})
	cmdSet.Set("stats", "suggest").MustRunner(squashErrors(requireConfig(st.Suggest)), &stats.SuggestFlags{})

	rpts := reports.New(env)
	cmdSet.Set("reports", "generate").MustRunner(squashErrors(rpts.Generate), &reports.GenerateFlags{})
	cmdSet.Set("reports", "locate").MustRunner(squashErrors(rpts.Locate), &reports.LocateFlags{})

	ui := tui.New(env, tui.WithLogToFile(logsToFile))
	cmdSet.Set("tui").MustRunner(squashErrors(requireConfig(ui.TUI)), &tui.Flags{})

	fnd := find.New(env)
	cmdSet.Set("find").MustRunner(squashErrors(requireConfig(fnd.Find)), &find.Flags{})

	cmdSet.AppendPreHooks(loggingHook)

	globals := subcmd.GlobalFlagSet()
	globals.MustRegisterFlagStruct(&globalFlags, nil, nil)
	cmdSet.WithGlobalFlags(globals)
	cmdSet.WithMain(mainWrapper)
	return cmdSet
}

func mainWrapper(ctx context.Context, cmdRunner func(ctx context.Context) error) error {

	debugpkg.SetGCPercent(globalFlags.GCPercent)

	switch globalFlags.Units.Value {
	case unitsDecimal:
		env.BytesPrinter = cli.DecimalBytes
	case unitsBinary:
		env.BytesPrinter = cli.BinaryBytes
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
		defer save() //nolint: errcheck
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic: %v\n", r)
			runtime.Stack(panicBuf, true)
			fmt.Println(string(panicBuf))
		}
	}()

	cfg, loc, err := iduconfig.Load(ctx, &globalFlags.Config)
	env.Config = cfg
	env.FS = globalFlags.FS
	ctx = cmdutil.ContextWithValue(ctx, cfg)
	ctx = cmdutil.ContextWithValue(ctx, loc)
	ctx = cli.WithConfigLoadError(ctx, err)

	var ln net.Listener
	if port := globalFlags.HTTP; len(port) > 0 {
		if ln, err = net.Listen("tcp", port); err != nil {
			return err
		}
		// gosec G114
		go http.Serve(ln, nil) //nolint:gosec,errcheck
	}

	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return cmdRunner(ctx)
}

func main() {
	newCommandSet().MustDispatch(context.Background())
}
