// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package openai_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/llm/openai"
)

// server is a fake OpenAI compatible server that records the requests it
// receives.
type server struct {
	*httptest.Server
	auth    http.Header
	request map[string]any
	reply   func(w http.ResponseWriter, r *http.Request)
}

func newServer(t *testing.T, reply func(w http.ResponseWriter, r *http.Request)) *server {
	t.Helper()
	s := &server{reply: reply}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.auth = r.Header.Clone()
		s.request = nil
		if r.Body != nil {
			buf, _ := io.ReadAll(r.Body)
			if len(buf) > 0 {
				if err := json.Unmarshal(buf, &s.request); err != nil {
					t.Errorf("invalid request body: %v: %s", err, buf)
				}
			}
		}
		s.reply(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func newClient(t *testing.T, url string, opts ...openai.Option) *openai.Client {
	t.Helper()
	c, err := openai.New(url, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const chatReply = `{
  "id": "c1", "object": "chat.completion", "created": 10, "model": "m",
  "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
    "role": "assistant", "content": "", "reasoning_content": "thinking",
    "tool_calls": [{"id": "call1", "type": "function", "function": {"name": "run", "arguments": "{\"code\":\"1\"}"}}]}}],
  "usage": {"prompt_tokens": 3, "completion_tokens": 4, "total_tokens": 7}
}`

func TestChat(t *testing.T) {
	ctx := context.Background()
	srv := chatServer(t)
	c := newClient(t, srv.URL+"/v1/", openai.WithAPIKey("secret"), openai.WithModel("default-model"))
	tool := openai.FunctionTool("run", "run code", json.RawMessage(`{"type":"object"}`))
	resp, err := c.Chat(ctx, openai.ChatRequest{
		Messages: []openai.Message{{Role: openai.RoleUser, Content: "hi"}},
		Tools:    []openai.Tool{tool},
		Extra:    map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false}},
	})
	if err != nil {
		t.Fatal(err)
	}
	check(t, srv.auth.Get("Authorization"), "Bearer secret")
	check(t, srv.request["model"], "default-model")
	if _, ok := srv.request["stream"]; ok {
		t.Errorf("stream should not be set: %v", srv.request)
	}
	kw, _ := srv.request["chat_template_kwargs"].(map[string]any)
	if got, ok := kw["enable_thinking"]; !ok || got != false {
		t.Errorf("extra field missing: %v", srv.request)
	}
	tools, _ := srv.request["tools"].([]any)
	if len(tools) != 1 {
		t.Errorf("tools missing: %v", srv.request)
	}
	if got, want := len(resp.Choices), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	msg := resp.Choices[0].Message
	check(t, msg.ReasoningContent, "thinking")
	if got, want := len(msg.ToolCalls), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	check(t, msg.ToolCalls[0].Func.Arguments, `{"code":"1"}`)
	check(t, resp.Usage.TotalTokens, 7)
}

func chatServer(t *testing.T) *server {
	return newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}
		fmt.Fprint(w, chatReply)
	})
}

func TestChatOptions(t *testing.T) {
	ctx := context.Background()
	srv := chatServer(t)

	// The model of the request takes precedence and a custom header carries
	// the key as is.
	c := newClient(t, srv.URL+"/v1", openai.WithAPIKey("secret"), openai.WithAuthHeader("api-key"))
	if _, err := c.Chat(ctx, openai.ChatRequest{Model: "other"}); err != nil {
		t.Fatal(err)
	}
	check(t, srv.request["model"], "other")
	check(t, srv.auth.Get("api-key"), "secret")
	if got := srv.auth.Get("Authorization"); got != "" {
		t.Errorf("unexpected Authorization header: %q", got)
	}

	// No key, no header.
	c = newClient(t, srv.URL+"/v1")
	if _, err := c.Chat(ctx, openai.ChatRequest{Model: "m"}); err != nil {
		t.Fatal(err)
	}
	if got := srv.auth.Get("Authorization"); got != "" {
		t.Errorf("unexpected Authorization header: %q", got)
	}

	// No model.
	if _, err := c.Chat(ctx, openai.ChatRequest{}); err == nil || !strings.Contains(err.Error(), "no model") {
		t.Errorf("unexpected error: %v", err)
	}

	// An extra field cannot replace a standard one.
	if _, err := c.Chat(ctx, openai.ChatRequest{Model: "m", Extra: map[string]any{"model": "x"}}); err == nil || !strings.Contains(err.Error(), "replaces") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestModels(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		fmt.Fprint(w, `{"object":"list","data":[{"id":"a","object":"model"},{"id":"b","object":"model"}]}`)
	})
	models, err := newClient(t, srv.URL+"/v1").Models(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(models), 2; got != want || models[1].ID != "b" {
		t.Errorf("got %v", models)
	}
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	var status int
	var body string
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	})
	c := newClient(t, srv.URL+"/v1", openai.WithModel("m"), openai.WithAPIKey("secret"))
	for _, tc := range []struct {
		status        int
		body          string
		code, message string
	}{
		{http.StatusUnauthorized, `{"error":{"message":"bad key","type":"auth","code":"invalid_api_key"}}`, "invalid_api_key", "bad key"},
		{http.StatusBadRequest, `{"error":{"message":"too long","code":400}}`, "400", "too long"},
		{http.StatusBadGateway, `upstream failed`, "", ""},
	} {
		status, body = tc.status, tc.body
		_, err := c.Chat(ctx, openai.ChatRequest{})
		var ae *openai.APIError
		if !errors.As(err, &ae) {
			t.Fatalf("unexpected error: %v", err)
		}
		if ae.StatusCode != tc.status || ae.Code != tc.code || ae.Message != tc.message {
			t.Errorf("unexpected error: %#v", ae)
		}
		if tc.message == "" && ae.Body != tc.body {
			t.Errorf("got %q, want %q", ae.Body, tc.body)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Errorf("error contains the key: %v", err)
		}
	}

	// An oversized response.
	status, body = http.StatusOK, chatReply
	c = newClient(t, srv.URL+"/v1", openai.WithModel("m"), openai.WithMaxResponseBytes(10))
	if _, err := c.Chat(ctx, openai.ChatRequest{}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSecurity(t *testing.T) {
	for _, tc := range []struct {
		url   string
		opts  []openai.Option
		error string
	}{
		{"http://example.com/v1", []openai.Option{openai.WithAPIKey("k")}, "plain http"},
		{"http://example.com/v1", []openai.Option{openai.WithAPIKey("k"), openai.WithAllowInsecure(true)}, ""},
		{"http://example.com/v1", nil, ""},
		{"https://example.com/v1", []openai.Option{openai.WithAPIKey("k")}, ""},
		{"http://localhost:8000/v1", []openai.Option{openai.WithAPIKey("k")}, ""},
		{"http://127.0.0.1:8000/v1", []openai.Option{openai.WithAPIKey("k")}, ""},
		{"http://[::1]:8000/v1", []openai.Option{openai.WithAPIKey("k")}, ""},
		{"ftp://example.com/v1", nil, "scheme"},
		{"https://user:pw@example.com/v1", nil, "credentials"},
		{"/v1", nil, "scheme"},
	} {
		_, err := openai.New(tc.url, tc.opts...)
		switch {
		case tc.error == "" && err != nil:
			t.Errorf("%v: unexpected error: %v", tc.url, err)
		case tc.error != "" && (err == nil || !strings.Contains(err.Error(), tc.error)):
			t.Errorf("%v: got %v, want an error containing %q", tc.url, err, tc.error)
		case err != nil && strings.Contains(err.Error(), "pw"):
			t.Errorf("%v: error contains the password: %v", tc.url, err)
		}
	}

	// Redirects are not followed, so that the key is not sent elsewhere.
	var redirected bool
	other := newServer(t, func(http.ResponseWriter, *http.Request) { redirected = true })
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+r.URL.Path, http.StatusTemporaryRedirect) //nolint:gosec // G710: a test of redirects
	})
	c := newClient(t, srv.URL+"/v1", openai.WithModel("m"), openai.WithAPIKey("secret"), openai.WithHTTPClient(http.DefaultClient))
	_, err := c.Chat(context.Background(), openai.ChatRequest{})
	var ae *openai.APIError
	if !errors.As(err, &ae) || ae.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("unexpected error: %v", err)
	}
	if redirected {
		t.Errorf("redirect was followed")
	}
	if http.DefaultClient.CheckRedirect != nil {
		t.Errorf("http.DefaultClient was modified")
	}
}

func check[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
