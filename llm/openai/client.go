// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package openai provides a client for the chat completions and models
// endpoints of the OpenAI API, as implemented by OpenAI compatible servers
// such as oMLX, llama-server, Ollama and vLLM, as well as hosted services.
//
// The client only sends requests and returns the model's responses: it
// never executes the tool calls that a model requests, which is left to
// the caller.
//
// The API key is sent only to the configured server: redirects are not
// followed, and sending a key over plain http to a host other than
// localhost is an error unless WithAllowInsecure is specified. The key is
// never included in an error.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultMaxResponseBytes is the maximum size of a response that is
	// not streamed, and of each event in a streamed response.
	DefaultMaxResponseBytes = 32 << 20

	authorizationHeader = "Authorization"
)

type options struct {
	httpClient    *http.Client
	apiKey        string
	authHeader    string
	model         string
	timeout       time.Duration
	allowInsecure bool
	maxResponse   int64
}

// Option represents an option to New.
type Option func(*options)

// WithHTTPClient specifies the http.Client to use, http.DefaultClient's
// transport is used by default. The Client uses a copy of hc that does
// not follow redirects.
func WithHTTPClient(hc *http.Client) Option {
	return func(o *options) {
		o.httpClient = hc
	}
}

// WithAPIKey specifies the API key to send with each request, see
// WithAuthHeader.
func WithAPIKey(key string) Option {
	return func(o *options) {
		o.apiKey = key
	}
}

// WithAuthHeader specifies the HTTP header that carries the API key. The
// default, Authorization, carries it as 'Bearer <key>'; any other header,
// eg. api-key or x-api-key, carries it as is.
func WithAuthHeader(header string) Option {
	return func(o *options) {
		o.authHeader = header
	}
}

// WithModel specifies the model to use for requests that do not specify
// one.
func WithModel(model string) Option {
	return func(o *options) {
		o.model = model
	}
}

// WithTimeout specifies the time allowed for each request, including the
// reading of a streamed response. There is no timeout by default, other
// than that of the context.
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
	}
}

// WithAllowInsecure allows the API key to be sent over plain http to a
// host other than localhost.
func WithAllowInsecure(allow bool) Option {
	return func(o *options) {
		o.allowInsecure = allow
	}
}

// WithMaxResponseBytes specifies the maximum size of a response, see
// DefaultMaxResponseBytes.
func WithMaxResponseBytes(n int64) Option {
	return func(o *options) {
		o.maxResponse = n
	}
}

// Client is a client for an OpenAI compatible API.
type Client struct {
	opts    options
	baseURL *url.URL
	hc      *http.Client
}

// New returns a Client for the API at baseURL, which includes the API's
// version prefix, eg. https://api.openai.com/v1 or
// http://127.0.0.1:8000/v1.
func New(baseURL string, opts ...Option) (*Client, error) {
	c := &Client{}
	c.opts.authHeader = authorizationHeader
	c.opts.maxResponse = DefaultMaxResponseBytes
	for _, fn := range opts {
		fn(&c.opts)
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("openai: %v: the scheme must be http or https", u.Redacted())
	}
	if u.Host == "" {
		return nil, fmt.Errorf("openai: %v: no host", u.Redacted())
	}
	if u.User != nil {
		return nil, fmt.Errorf("openai: %v: credentials must not be included in the url", u.Redacted())
	}
	if c.opts.apiKey != "" && u.Scheme == "http" && !isLoopback(u.Hostname()) && !c.opts.allowInsecure {
		return nil, fmt.Errorf("openai: %v: an API key would be sent over plain http to a host other than localhost, use https or allow insecure connections", u)
	}
	if c.opts.authHeader == "" {
		c.opts.authHeader = authorizationHeader
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	c.baseURL = u
	hc := http.Client{}
	if c.opts.httpClient != nil {
		hc = *c.opts.httpClient
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	c.hc = &hc
	return c, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// BaseURL returns the base URL of the API.
func (c *Client) BaseURL() string {
	return c.baseURL.String()
}

// Model returns the default model, see WithModel.
func (c *Client) Model() string {
	return c.opts.model
}

// Models returns the models listed by the server.
func (c *Client) Models(ctx context.Context) ([]Model, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	resp, err := c.do(ctx, http.MethodGet, "/models", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var models struct {
		Data []Model `json:"data"`
	}
	if err := c.decode(resp, &models); err != nil {
		return nil, err
	}
	return models.Data, nil
}

// Chat returns the completion for req.
func (c *Client) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	req.Stream, req.StreamOptions = false, nil
	resp, err := c.post(ctx, "/chat/completions", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var cr ChatResponse
	if err := c.decode(resp, &cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.opts.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.opts.timeout)
}

func (c *Client) post(ctx context.Context, path string, req ChatRequest) (*http.Response, error) {
	if req.Model == "" {
		req.Model = c.opts.model
	}
	if req.Model == "" {
		return nil, errors.New("openai: no model specified")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	return c.do(ctx, http.MethodPost, path, body)
}

// do sends a request and returns the response if its status is 2xx,
// otherwise it returns an *APIError.
func (c *Client) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	u := *c.baseURL
	u.Path += path
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	if c.opts.apiKey != "" {
		if strings.EqualFold(c.opts.authHeader, authorizationHeader) {
			req.Header.Set(authorizationHeader, "Bearer "+c.opts.apiKey)
		} else {
			req.Header.Set(c.opts.authHeader, c.opts.apiKey)
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, c.apiError(resp, method, u.Path)
	}
	return resp, nil
}

func (c *Client) decode(resp *http.Response, v any) error {
	buf, err := io.ReadAll(io.LimitReader(resp.Body, c.opts.maxResponse+1))
	if err != nil {
		return fmt.Errorf("openai: reading response: %w", err)
	}
	if int64(len(buf)) > c.opts.maxResponse {
		return fmt.Errorf("openai: response exceeds %v bytes", c.opts.maxResponse)
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return fmt.Errorf("openai: decoding response: %w", err)
	}
	return nil
}
