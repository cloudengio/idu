# Package [cloudeng.io/cmd/idu/usernames](https://pkg.go.dev/cloudeng.io/cmd/idu/usernames?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/usernames)](https://goreportcard.com/report/cloudeng.io/cmd/idu/usernames)

```go
import cloudeng.io/cmd/idu/usernames
```

Package usernames maps user and group ids, as recorded by idu and used in
its statistics, to and from their names, caching the results of the lookups.

## Variables
### IDM
```go
IDM *userid.IDManager

```
IDM is the userid.IDManager used by Manager.



## Types
### Type IDManager
```go
type IDManager struct {
	// contains filtered or unexported fields
}
```
IDManager maps user and group ids to and from their names; ids that cannot
be looked up are mapped to their decimal representations.

### Variables
### Manager
```go
Manager IDManager

```
Manager is the IDManager used by idu.



### Methods

```go
func (um *IDManager) GIDForName(name string) (int64, error)
```
GIDForName returns the id of the group named name, which may be a decimal
id.


```go
func (um *IDManager) NameForGID(gid int64) string
```
NameForGID returns the name of the group with id gid.


```go
func (um *IDManager) NameForUID(uid int64) string
```
NameForUID returns the name of the user with id uid.


```go
func (um *IDManager) UIDForName(name string) (int64, error)
```
UIDForName returns the id of the user named name, which may be a decimal id.







