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
        summary: view the statistics stored in a file created using the compute command. If no filename is specified then the latest statistics for the filesystem specified by --fs are displayed, otherwise the named file is displayed, a relative filename that does not exist is looked for in the stats directory, which is specified by --stats-dir or is the stats directory of the filesystem specified by --fs.
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
        summary:  generate reports in a variety of formats, including tsv, json and markdown from the statistics stored in the specified file.
        arguments:
          - <filename>
          - ...

      - name: locate
        summary: locate the last n sets of reports in a given directory as a json array of filenames. This is intended to be used by other scripts that analyze the reports.
        arguments:
          - <report-directory>

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

	alz := analyze.New(env, newAnalyzeProgress)
	cmdSet.Set("analyze").MustRunner(squashErrors(requireConfig(alz.Analyze)), &analyze.Flags{})
	cmdSet.Set("errors").MustRunner(squashErrors(requireConfig(alz.Errors)), &analyze.ErrorFlags{})
	cmdSet.Set("logs").MustRunner(squashErrors(requireConfig(alz.Logs)), &analyze.LogFlags{})
	cmdSet.Set("database", "locate").MustRunner(squashErrors(requireConfig(alz.Locate)), &analyze.LocateFlags{})

	st := stats.New(env)
	cmdSet.Set("stats", "compute").MustRunner(squashErrors(requireConfig(st.Compute)), &stats.ComputeFlags{})
	cmdSet.Set("stats", "view").MustRunner(squashErrors(st.View), &stats.ViewFlags{})
	cmdSet.Set("stats", "suggest").MustRunner(squashErrors(requireConfig(st.Suggest)), &stats.SuggestFlags{})

	rpts := reports.New(env)
	cmdSet.Set("reports", "generate").MustRunner(squashErrors(rpts.Generate), &reports.GenerateFlags{})
	cmdSet.Set("reports", "locate").MustRunner(squashErrors(rpts.Locate), &reports.LocateFlags{})

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
