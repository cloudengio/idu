# Package [cloudeng.io/cmd/idu/internal/database/types](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/database/types?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/database/types)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/database/types)

```go
import cloudeng.io/cmd/idu/internal/database/types
```


## Functions
### Func Decode
```go
func Decode[T any](buf []byte, v *T) error
```

### Func Encode
```go
func Encode[T any](buf *bytes.Buffer, v T) error
```



## Types
### Type ErrorPayload
```go
type ErrorPayload struct {
	When    time.Time
	Key     string
	Payload []byte
}
```


### Type LogPayload
```go
type LogPayload struct {
	Start, Stop time.Time
	Payload     []byte
}
```


### Type StatsPayload
```go
type StatsPayload struct {
	When    time.Time
	Payload []byte
}
```





