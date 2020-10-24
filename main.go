// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/cmdutil/profiling"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/file/diskusage"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	cmdSet       *subcmd.CommandSet
	globalFlags  GlobalFlags
	globalConfig *config.Config
	panicBuf     = make([]byte, 1024*1024)
	bytesPrinter func(size int64) (float64, string)
)

type GlobalFlags struct {
	ExitProfile profiling.ProfileFlag `subcmd:"exit-profile,,'write a profile on exit; the format is <profile-name>:<file> and the flag may be repeated to request multiple profile types, use cpu to request cpu profiling in addition to predefined profiles in runtime/pprof'"`
	Human       bool                  `subcmd:"h,true,show sizes in human readable form"`
	ConfigFile  string                `subcmd:"config,$HOME/.idu.yml,configuration file"`
	Units       string                `subcmd:"units,decimal,display usage in decimal (KB) or binary (KiB) formats"`
	Verbose     int                   `subcmd:"v,0,higher values show more debugging output"`
}

func init() {
	analyzeFlagSet := subcmd.MustRegisterFlagStruct(&analyzeFlags{}, nil, nil)
	summaryFlagSet := subcmd.MustRegisterFlagStruct(&summaryFlags{}, nil, nil)
	userFlagSet := subcmd.MustRegisterFlagStruct(&userFlags{}, nil, nil)
	groupFlagSet := subcmd.MustRegisterFlagStruct(&groupFlags{}, nil, nil)
	findFlagSet := subcmd.MustRegisterFlagStruct(&findFlags{}, nil, nil)
	lsFlagSet := subcmd.MustRegisterFlagStruct(&lsFlags{}, nil, nil)
	eraseFlagSet := subcmd.MustRegisterFlagStruct(&eraseFlags{}, nil, nil)

	analyzeCmd := subcmd.NewCommand("analyze", analyzeFlagSet, analyze)
	analyzeCmd.Document("analyze the file system to build a database of file counts, disk usage etc", "<directory/prefix>+")

	summaryCmd := subcmd.NewCommand("summary", summaryFlagSet, summary, subcmd.ExactlyNumArguments(1))
	summaryCmd.Document("summarize file count and disk usage")

	userSummaryCmd := subcmd.NewCommand("user", userFlagSet, userSummary, subcmd.AtLeastNArguments(1))
	userSummaryCmd.Document("summarize file count and disk usage on a per user basis", "<prefix> <users>...")

	groupSummaryCmd := subcmd.NewCommand("group", groupFlagSet, groupSummary, subcmd.AtLeastNArguments(1))
	groupSummaryCmd.Document("summarize file count and disk usage on a per group basis", "<prefix> <groups>...")

	findCmd := subcmd.NewCommand("find", findFlagSet, find)
	findCmd.Document("find prefixes/files in statistics database")

	lsrCmd := subcmd.NewCommand("lsr", lsFlagSet, lsr, subcmd.AtLeastNArguments(1))
	lsrCmd.Document("list the contents of the database")

	eraseCmd := subcmd.NewCommand("erase-database", eraseFlagSet, erase, subcmd.ExactlyNumArguments(1))
	eraseCmd.Document("erase the file statistics database")

	configFlagSet := subcmd.MustRegisterFlagStruct(&configFlags{}, nil, nil)
	configCmd := subcmd.NewCommand("config", configFlagSet, configManager, subcmd.WithoutArguments())
	configCmd.Document("describe the current configuration")

	refreshStatsFlagSet := subcmd.NewFlagSet()
	refreshStatsCmd := subcmd.NewCommand("refresh-stats", refreshStatsFlagSet, refreshStats, subcmd.ExactlyNumArguments(1))
	refreshStatsCmd.Document("refresh statistics by recalculating them over the entire database")

	errorsFlagSet := subcmd.NewFlagSet()
	errorsCmd := subcmd.NewCommand("errors", errorsFlagSet, listErrors, subcmd.ExactlyNumArguments(1))
	errorsCmd.Document("list the contents of the errors database")

	cmdSet = subcmd.NewCommandSet(analyzeCmd, configCmd, eraseCmd, errorsCmd, lsrCmd, findCmd, summaryCmd, userSummaryCmd, groupSummaryCmd, refreshStatsCmd)
	cmdSet.Document(`idu: analyze file systems to create a database of per-file and aggregate size stastistics to support incremental updates and subsequent interrogation. Local and cloud based filesystems are contemplated. See https://github.com/cloudengio/blob/master/idu/README.md for full details.`)

	globals := subcmd.GlobalFlagSet()
	globals.MustRegisterFlagStruct(&globalFlags, nil, nil)
	cmdSet.WithGlobalFlags(globals)
	cmdSet.WithMain(mainWrapper)
}

func mainWrapper(ctx context.Context, cmdRunner func() error) error {
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
	return cmdRunner()
}

func main() {
	cmdSet.MustDispatch(context.Background())
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

func fsize(size int64) string {
	if globalFlags.Human {
		f, u := bytesPrinter(size)
		return printer.Sprintf("%0.3f %s", f, u)
	}
	return printer.Sprintf("%v", size)
}
