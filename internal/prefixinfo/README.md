# Package [cloudeng.io/cmd/idu/internal/prefixinfo](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/prefixinfo?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/prefixinfo)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/prefixinfo)

```go
import cloudeng.io/cmd/idu/internal/prefixinfo
```


## Functions
### Func NewSysInfo
```go
func NewSysInfo(uid, gid int64, dev, ino uint64, blocks int64) any
```
NewSysInfo is intended to be used by tests.



## Types
### Type IDSanner
```go
type IDSanner interface {
	Next() bool
	Info() file.Info
}
```
IDScanner allows for iterating over files that belong to a particular user
or group.


### Type Stats
```go
type Stats struct {
	ID           uint32
	Files        int64 // number of files
	Prefixes     int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
	PrefixBytes  int64 // total size of prefixes
}
```

### Methods

```go
func (s *Stats) AppendBinary(data []byte) []byte
```


```go
func (s *Stats) DecodeBinary(data []byte) []byte
```


```go
func (s *Stats) MarshalBinary() (data []byte, err error)
```


```go
func (s *Stats) UnmarshalBinary(data []byte) error
```




### Type StatsList
```go
type StatsList []Stats
```

### Methods

```go
func (sl StatsList) AppendBinary(data []byte) []byte
```


```go
func (sl *StatsList) DecodeBinary(data []byte) []byte
```


```go
func (sl StatsList) MarshalBinary() (data []byte, err error)
```


```go
func (sl *StatsList) UnmarshalBinary(data []byte) error
```




### Type T
```go
type T struct {
	// contains filtered or unexported fields
}
```

### Functions

```go
func New(pathname string, info file.Info) T
```
New creates a new PrefixInfo for the supplied file.Info. It assumes that the
supplied file.Info contains a file.XAttr in its Sys() value.



### Methods

```go
func (pi *T) AppendBinary(buf *bytes.Buffer) error
```


```go
func (pi *T) AppendInfo(entry file.Info)
```


```go
func (pi *T) AppendInfoList(entries file.InfoList)
```


```go
func (pi T) Blocks() int64
```


```go
func (pi T) DevIno() (device, inode uint64)
```


```go
func (pi T) FilesOnly() file.InfoList
```


```go
func (pi *T) GroupIDScan(id int64) (IDSanner, error)
```
GroupIDScan returns an IDSanner for the supplied group id.


```go
func (pi T) InfoList() file.InfoList
```
Info returns the list of file.Info's available for this prefix. NOTE that
these may contain directories, ie. entries for which IsDir is true.


```go
func (pi *T) MarshalBinary() ([]byte, error)
```


```go
func (pi T) ModTime() time.Time
```


```go
func (pi T) Mode() fs.FileMode
```


```go
func (pi T) PrefixesOnly() file.InfoList
```


```go
func (pi *T) SetInfoList(entries file.InfoList)
```


```go
func (pi T) Size() int64
```


```go
func (pi T) Type() fs.FileMode
```


```go
func (pi T) Unchanged(npi T) bool
```


```go
func (pi T) UnchangedInfo(info file.Info) bool
```


```go
func (pi *T) UnmarshalBinary(data []byte) error
```


```go
func (pi T) UserGroup() (uid, gid int64)
```


```go
func (pi *T) UserIDScan(id int64) (IDSanner, error)
```
UserIDScan returns an IDSanner for the supplied user id. It can only be used
after Finalize has been called.


```go
func (pi T) XAttr() file.XAttr
```


```go
func (pi T) XAttrInfo(fi file.Info) file.XAttr
```







### TODO
- cnicolaou: parametize for posix and non-posix (ie. numeric vs string) UID/GID.




