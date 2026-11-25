# Package [cloudeng.io/cmd/idu/llm/openai](https://pkg.go.dev/cloudeng.io/cmd/idu/llm/openai?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/llm/openai)](https://goreportcard.com/report/cloudeng.io/cmd/idu/llm/openai)

```go
import cloudeng.io/cmd/idu/llm/openai
```

Package openai provides a client for the chat completions and models
endpoints of the OpenAI API, as implemented by OpenAI compatible servers
such as oMLX, llama-server, Ollama and vLLM, as well as hosted services.

The client only sends requests and returns the model's responses: it never
executes the tool calls that a model requests, which is left to the caller.

The API key is sent only to the configured server: redirects are not
followed, and sending a key over plain http to a host other than localhost
is an error unless WithAllowInsecure is specified. The key is never included
in an error.

## Constants
### DefaultMaxResponseBytes
```go
// DefaultMaxResponseBytes is the maximum size of a response that is
// not streamed, and of each event in a streamed response.
const DefaultMaxResponseBytes = 32 << 20
```



## Types
### Type APIError
```go
type APIError struct {
	// StatusCode is the HTTP status, or zero for an error reported in a
	// streamed response.
	StatusCode int
	// Method and Path identify the request.
	Method, Path string
	// Message, Type, Param and Code are those of the error object
	// returned by the server, if any.
	Message, Type, Param, Code string
	// Body is the body of the response if it does not contain an error
	// object, truncated.
	Body string
}
```
APIError is returned for a response whose status is not 2xx, or for an error
reported in a streamed response.

### Methods

```go
func (e *APIError) Error() string
```




### Type Accumulator
```go
type Accumulator struct {
	// contains filtered or unexported fields
}
```
Accumulator assembles the chunks of a streamed response into a ChatResponse:
the content, reasoning and the fragments of the arguments of each tool call
are concatenated.

### Methods

```go
func (a *Accumulator) Add(chunk *ChatChunk)
```
Add adds chunk to the response.


```go
func (a *Accumulator) Response() *ChatResponse
```
Response returns the response assembled so far.




### Type ChatChunk
```go
type ChatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	// Usage is set, in the last chunk, if StreamOptions.IncludeUsage is.
	Usage *Usage `json:"usage,omitempty"`
}
```
ChatChunk is a fragment of a streamed ChatResponse.


### Type ChatRequest
```go
type ChatRequest struct {
	// Model defaults to that of the Client, see WithModel.
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Tools    []Tool    `json:"tools,omitempty"`
	// ToolChoice is one of "none", "auto" or "required", or an object that
	// names a function.
	ToolChoice        any             `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool           `json:"parallel_tool_calls,omitempty"`
	Temperature       *float64        `json:"temperature,omitempty"`
	TopP              *float64        `json:"top_p,omitempty"`
	MaxTokens         int             `json:"max_tokens,omitempty"`
	Stop              []string        `json:"stop,omitempty"`
	Seed              *int64          `json:"seed,omitempty"`
	ResponseFormat    *ResponseFormat `json:"response_format,omitempty"`
	// Stream and StreamOptions are set by ChatStream.
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
	// Extra holds fields that are specific to a server, eg. oMLX's and
	// llama-server's chat_template_kwargs, or reasoning_effort. They are
	// added to the request but cannot replace the fields above.
	Extra map[string]any `json:"-"`
}
```
ChatRequest is a request for a chat completion.

### Methods

```go
func (r ChatRequest) MarshalJSON() ([]byte, error)
```
MarshalJSON implements json.Marshaler, adding r.Extra to the request.




### Type ChatResponse
```go
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}
```
ChatResponse is the response to a ChatRequest.


### Type Choice
```go
type Choice struct {
	Index   int     `json:"index"`
	Message Message `json:"message"`
	// FinishReason is eg. stop, length or tool_calls.
	FinishReason string `json:"finish_reason"`
}
```
Choice is one of the completions returned for a request.


### Type ChunkChoice
```go
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Message `json:"delta"`
	FinishReason string  `json:"finish_reason"`
}
```
ChunkChoice is the fragment of a Choice in a ChatChunk.


### Type Client
```go
type Client struct {
	// contains filtered or unexported fields
}
```
Client is a client for an OpenAI compatible API.

### Functions

```go
func New(baseURL string, opts ...Option) (*Client, error)
```
New returns a Client for the API at baseURL, which includes the API's
version prefix, eg. https://api.openai.com/v1 or http://127.0.0.1:8000/v1.



### Methods

```go
func (c *Client) BaseURL() string
```
BaseURL returns the base URL of the API.


```go
func (c *Client) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error)
```
Chat returns the completion for req.


```go
func (c *Client) ChatStream(ctx context.Context, req ChatRequest) iter.Seq2[*ChatChunk, error]
```
ChatStream returns the completion for req as a sequence of chunks, read from
a stream of server-sent events. The sequence ends after the last chunk or
the first error; the stream is closed if the loop over it is ended early.
Use an Accumulator to assemble the chunks into a ChatResponse.


```go
func (c *Client) Model() string
```
Model returns the default model, see WithModel.


```go
func (c *Client) Models(ctx context.Context) ([]Model, error)
```
Models returns the models listed by the server.




### Type FunctionCall
```go
type FunctionCall struct {
	Name string `json:"name,omitempty"`
	// Arguments is the JSON encoded arguments as generated by the model,
	// which may not be valid JSON, or match the tool's parameters, and
	// must be validated before use.
	Arguments string `json:"arguments"`
}
```
FunctionCall is the function, and its arguments, of a ToolCall.


### Type FunctionDefinition
```go
type FunctionDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// Parameters is the JSON schema for the function's arguments.
	Parameters json.RawMessage `json:"parameters,omitempty"`
	Strict     bool            `json:"strict,omitempty"`
}
```
FunctionDefinition defines a function tool.


### Type JSONSchemaFormat
```go
type JSONSchemaFormat struct {
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
	Strict bool            `json:"strict,omitempty"`
}
```
JSONSchemaFormat is the schema for a ResponseFormat of type json_schema.


### Type Message
```go
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`
	// Name optionally identifies the author of a message.
	Name string `json:"name,omitempty"`
	// ToolCalls are the tool calls requested by an assistant message.
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	// ToolCallID is the ID of the call that a tool message is the result
	// of.
	ToolCallID string `json:"tool_call_id,omitempty"`
	// ReasoningContent is the reasoning, or thinking, that some servers,
	// eg. llama-server and oMLX, return separately from the content.
	// Servers differ as to whether they accept it in a request.
	ReasoningContent string `json:"reasoning_content,omitempty"`
}
```
Message is a message in a chat. Only the text form of content is supported.


### Type Model
```go
type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}
```
Model is a model listed by the server.


### Type Option
```go
type Option func(*options)
```
Option represents an option to New.

### Functions

```go
func WithAPIKey(key string) Option
```
WithAPIKey specifies the API key to send with each request, see
WithAuthHeader.


```go
func WithAllowInsecure(allow bool) Option
```
WithAllowInsecure allows the API key to be sent over plain http to a host
other than localhost.


```go
func WithAuthHeader(header string) Option
```
WithAuthHeader specifies the HTTP header that carries the API key. The
default, Authorization, carries it as 'Bearer <key>'; any other header, eg.
api-key or x-api-key, carries it as is.


```go
func WithHTTPClient(hc *http.Client) Option
```
WithHTTPClient specifies the http.Client to use, http.DefaultClient's
transport is used by default. The Client uses a copy of hc that does not
follow redirects.


```go
func WithMaxResponseBytes(n int64) Option
```
WithMaxResponseBytes specifies the maximum size of a response, see
DefaultMaxResponseBytes.


```go
func WithModel(model string) Option
```
WithModel specifies the model to use for requests that do not specify one.


```go
func WithTimeout(d time.Duration) Option
```
WithTimeout specifies the time allowed for each request, including the
reading of a streamed response. There is no timeout by default, other than
that of the context.




### Type ResponseFormat
```go
type ResponseFormat struct {
	// Type is one of text, json_object or json_schema.
	Type       string            `json:"type"`
	JSONSchema *JSONSchemaFormat `json:"json_schema,omitempty"`
}
```
ResponseFormat constrains the format of the model's output.


### Type Role
```go
type Role string
```
Role is the role of the author of a message.

### Constants
### RoleSystem, RoleDeveloper, RoleUser, RoleAssistant, RoleTool
```go
const (
	RoleSystem    Role = "system"
	RoleDeveloper Role = "developer"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)
```
The roles.




### Type StreamOptions
```go
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}
```
StreamOptions are the options for a streamed response.


### Type Tool
```go
type Tool struct {
	Type string             `json:"type"`
	Func FunctionDefinition `json:"function"`
}
```
Tool is a tool that the model may call.

### Functions

```go
func FunctionTool(name, description string, parameters json.RawMessage) Tool
```
FunctionTool returns a function Tool whose arguments are described by the
JSON schema in parameters.




### Type ToolCall
```go
type ToolCall struct {
	// Index identifies the call that a streamed fragment belongs to, it is
	// only set in a ChatChunk.
	Index *int         `json:"index,omitempty"`
	ID    string       `json:"id,omitempty"`
	Type  string       `json:"type,omitempty"`
	Func  FunctionCall `json:"function"`
}
```
ToolCall is a call of a function tool requested by the model.


### Type Usage
```go
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
```
Usage reports the number of tokens used by a request.





