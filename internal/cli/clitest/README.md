# Package [cloudeng.io/cmd/idu/internal/cli/clitest](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/cli/clitest?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/cli/clitest)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/cli/clitest)

```go
import cloudeng.io/cmd/idu/internal/cli/clitest
```

Package clitest provides support for testing idu's command groups.

## Functions
### Func CaptureStdout
```go
func CaptureStdout(t *testing.T, fn func() error) (string, error)
```
CaptureStdout returns the output written to os.Stdout by fn.

### Func CreateHardlinkTree
```go
func CreateHardlinkTree(t *testing.T, root string)
```
CreateHardlinkTree creates a tree with hard links to files in sibling
directories whose names share a string prefix (a, a-b, ab).

### Func EnsureDirModTimeChanged
```go
func EnsureDirModTimeChanged(t *testing.T, dir string, fn func())
```
EnsureDirModTimeChanged runs fn and advances dir's modification time if the
filesystem timestamp did not advance (e.g. due to coarse clock ticks).

### Func LinkUnchanged
```go
func LinkUnchanged(t *testing.T, root string)
```
LinkUnchanged creates a hardlink in a, which is therefore rescanned, to b/h,
a file in b, which is not rescanned since it is unchanged, and which had
a link count of 1 when last scanned. a/hl precedes b/h and is counted,
b/h is now a hardlink.

### Func NewHardlinkTree
```go
func NewHardlinkTree(t *testing.T) (tmpDir, root string)
```
NewHardlinkTree creates the tree created by CreateHardlinkTree in a new
temporary directory, returning the directory and the root of the tree.

### Func NumDirsAndFiles
```go
func NumDirsAndFiles(paths []string) (d, f int64)
```

### Func OpenPrefixRaw
```go
func OpenPrefixRaw(ctx context.Context, env *cli.Env, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Database, error)
```
OpenPrefixRaw is like cli.Env.OpenPrefixReadOnly but returns a raw,
writable, database handle, see OpenRawDB.

### Func OpenRawDB
```go
func OpenRawDB(_ context.Context, path string) (idudb.Database, error)
```
OpenRawDB opens the database at path for reading and writing without going
through idudb. It must only be used by tests that need to inject fixtures or
corrupt entries that cannot be created via idudb.

### Func WithConfig
```go
func WithConfig(t *testing.T, env *cli.Env, cfg iduconfig.T)
```
WithConfig sets env.Config for the duration of the test.

### Func WithFS
```go
func WithFS(t *testing.T, env *cli.Env, name string)
```
WithFS sets env.FS, ie. the --fs flag, for the duration of the test.

### Func WriteHardlinkConfig
```go
func WriteHardlinkConfig(t *testing.T, dir, root string, dedup iduconfig.HardlinkDedup) iduconfig.T
```
WriteHardlinkConfig writes a configuration for root, with its database in
dir, that uses the hardlink deduplication strategy dedup.



## Types
### Type TestTree
```go
type TestTree struct {
	Root string
	// contains filtered or unexported fields
}
```

### Functions

```go
func NewTestTree(parent string, depth, breadth, nfiles int) *TestTree
```


```go
func SetupAnalyze(t *testing.T) (tmpDir, config, prefix string, tt *TestTree)
```
SetupAnalyze creates a TestTree, with its base content, and a configuration
file for it, named testtree, that excludes part of it. It returns the
temporary directory containing them, the configuration file and the root of
the tree.



### Methods

```go
func (tt *TestTree) Additional() []string
```


```go
func (tt *TestTree) Base() []string
```


```go
func (tt *TestTree) CreateAdditional(depth, nfiles, ndirs int) error
```


```go
func (tt *TestTree) CreateBase() error
```


```go
func (tt *TestTree) DeleteAdditional() error
```







