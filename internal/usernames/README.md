# Package [cloudeng.io/cmd/idu/internal/usernames](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/usernames?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/usernames)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/usernames)

```go
import cloudeng.io/cmd/idu/internal/usernames
```


## Variables
### IDM
```go
IDM *userid.IDManager

```



## Types
### Type IDManager
```go
type IDManager struct {
	// contains filtered or unexported fields
}
```

### Variables
### Manager
```go
Manager IDManager

```



### Methods

```go
func (um *IDManager) GIDForName(name string) (int64, error)
```


```go
func (um *IDManager) NameForGID(gid int64) string
```


```go
func (um *IDManager) NameForUID(uid int64) string
```


```go
func (um *IDManager) UIDForName(name string) (int64, error)
```







