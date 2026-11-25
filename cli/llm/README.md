# Package [cloudeng.io/cmd/idu/cli/llm](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/llm?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/llm)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/llm)

```go
import cloudeng.io/cmd/idu/cli/llm
```

Package llm provides the llm commands, which check access to the OpenAI
compatible API configured by the llm section of idu's configuration,
by listing its models and sending it a prompt. The API key is read from the
environment variable named by llm.api_key_env, which may be set in a .env
configuration file, see iduconfig.Environment.

## Functions
### Func NewClient
```go
func NewClient(cfg iduconfig.LLM, env *iduconfig.Environment) (*openai.Client, error)
```
NewClient returns a client for the API configured by cfg, with the API key,
if any, read from env.



## Types
### Type ChatFlags
```go
type ChatFlags struct {
	Model     string `subcmd:"model,,'the model to use, defaults to llm.model in the configuration'"`
	System    string `subcmd:"system,,a system prompt"`
	MaxTokens int    `subcmd:"max-tokens,0,'the maximum number of tokens to generate, 0 for the default of the server'"`
	Stream    bool   `subcmd:"stream,true,stream the response as it is generated"`
	Reasoning bool   `subcmd:"reasoning,false,'display the reasoning of the model on stderr, if the server returns it separately'"`
}
```
ChatFlags are the flags for the llm chat command.


### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the llm commands.

### Functions

```go
func New(env *cli.Env) *Commands
```
New returns the Commands for env.



### Methods

```go
func (c *Commands) Chat(ctx context.Context, values any, args []string) error
```
Chat implements the llm chat command, values must be a *ChatFlags.
The prompt is the arguments, or stdin if the only argument is -.


```go
func (c *Commands) Models(ctx context.Context, _ any, _ []string) error
```
Models implements the llm models command.




### Type ModelsFlags
```go
type ModelsFlags struct{}
```
ModelsFlags are the flags for the llm models command.





