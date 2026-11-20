# Package [cloudeng.io/cmd/idu/cli/internal/statedir](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/internal/statedir?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/internal/statedir)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/internal/statedir)

```go
import cloudeng.io/cmd/idu/cli/internal/statedir
```

Package statedir manages directories of timestamped entries, files or
directories, such as stats snapshots and reports, together with a symbolic
link, named latest, to the most recent entry.

## Constants
### LatestName
```go
const LatestName = "latest"
```
LatestName is the name of the link to the most recent entry.



## Variables
### ErrNoSymlinks
```go
ErrNoSymlinks = errors.New("symbolic links are not supported")

```
ErrNoSymlinks is returned, wrapped, by SetLatest when the latest link cannot
be created because symbolic links are not supported, eg. on Windows without
developer mode or administrator privileges.



## Types
### Type Dir
```go
type Dir struct {
	// contains filtered or unexported fields
}
```
Dir is a directory of entries whose names are their timestamps, formatted
using a time layout, followed by an optional extension.

### Functions

```go
func New(root, layout, ext string, legacy ...string) Dir
```
New returns a Dir for the directory root, whose entries are named using
layout and ext. Entries whose names use any of the legacy layouts are also
recognized by Entries, but new entries are always named using layout.



### Methods

```go
func (d Dir) Entries() ([]Entry, error)
```
Entries returns the entries in the directory, most recent first. Names that
do not match the layout and extension, such as the latest link, are ignored.
A directory that does not exist has no entries.


```go
func (d Dir) LatestPath() string
```
LatestPath returns the path of the link to the most recent entry.


```go
func (d Dir) MkdirAll(when time.Time, subdirs ...string) (string, error)
```
MkdirAll creates the entry for when as a directory, together with the
supplied sub-directories within it, and returns its path. It does not update
the latest link, see SetLatest.


```go
func (d Dir) Name(when time.Time) string
```
Name returns the name of the entry for when, which is formatted in local
time.


```go
func (d Dir) Path(when time.Time) string
```
Path returns the path of the entry for when.


```go
func (d Dir) Root() string
```
Root returns the directory.


```go
func (d Dir) SetLatest(when time.Time) error
```
SetLatest makes the entry for when the latest entry by replacing the latest
link. It returns an error that wraps ErrNoSymlinks if the link could not be
created because symbolic links are not supported.


```go
func (d Dir) WriteFile(when time.Time, data []byte) (string, error)
```
WriteFile writes data as the entry for when and makes it the latest entry.
Existing entries are never overwritten: if there is already an entry
with the same name, since names have the resolution of the layout, eg.
a millisecond, the entry is written for the first subsequent time, at that
resolution, for which there is none. The entry is written to a temporary
file that is then renamed so that readers never see a partially written
entry. Where symbolic links are not supported the latest entry is a copy
instead.




### Type Entry
```go
type Entry struct {
	When  time.Time
	Name  string
	Path  string
	IsDir bool
}
```
Entry is an entry in a Dir.





