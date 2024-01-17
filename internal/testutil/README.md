# Package [cloudeng.io/cmd/idu/internal/testutil](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/testutil?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/testutil)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/testutil)

```go
import cloudeng.io/cmd/idu/internal/testutil
```


## Functions
### Func TestdataIDCombinations
```go
func TestdataIDCombinations(modTime time.Time, mode fs.FileMode, uid, gid int64, inode uint64) (ug00, ug10, ug01, ug11, ugOther []file.Info)
```
IDCombinations returns 5 sets of file.Info values with differing
combinations of uid and gid. ug00 has uid, gid for both files ug10 has
uid+1, gid for the second file ug01 has uid, gid+1 for the second file ug11
has uid+1, gid+1 for the second file ugOther has uid+1, gid+1 for both files

### Func TestdataIDCombinationsDirs
```go
func TestdataIDCombinationsDirs(modTime time.Time, uid, gid int64, inode uint64) (ug00, ug10, ug01, ug11, ugOther []file.Info)
```

### Func TestdataIDCombinationsFiles
```go
func TestdataIDCombinationsFiles(modTime time.Time, uid, gid int64, inode uint64) (ug00, ug10, ug01, ug11, ugOther []file.Info)
```

### Func TestdataNewInfo
```go
func TestdataNewInfo(name string, size, blocks int64, mode fs.FileMode, modTime time.Time, uid, gid int64, device, inode uint64) file.Info
```

### Func TestdataNewPrefixInfo
```go
func TestdataNewPrefixInfo(name string, size, blocks int64, mode fs.FileMode, modTime time.Time, uid, gid int64, dev, inode uint64) prefixinfo.T
```




