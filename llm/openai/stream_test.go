// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package openai_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/llm/openai"
)

// streamed is a stream with a keep-alive comment, an event split across
// two data lines, two tool calls whose arguments arrive in fragments and
// a final usage chunk.
var streamed = strings.Join([]string{
	": keep-alive",
	"",
	`data: {"id":"s1","model":"m","created":5,"choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"hmm"}}]}`,
	"",
	`data: {"id":"s1","choices":[{"index":0,"delta":{"content":"Hel"}}]}`,
	"",
	`data: {"id":"s1","choices":[{"index":0,`,
	`data: "delta":{"content":"lo"}}]}`,
	"",
	`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c0","type":"function","function":{"name":"run","arguments":"{\"co"}}]}}]}`,
	"",
	`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"c1","type":"function","function":{"name":"other","arguments":"{}"}}]}}]}`,
	"",
	`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"de\":1}"}}]}}]}`,
	"",
	`event: ignored`,
	`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
	"",
	`data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`,
	"",
	"data: [DONE]",
	"",
}, "\r\n")

func streamServer(t *testing.T, body string) *server {
	return newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, body)
	})
}

func TestChatStream(t *testing.T) {
	srv := streamServer(t, streamed)
	c := newClient(t, srv.URL+"/v1", openai.WithModel("m"))
	var acc openai.Accumulator
	n := 0
	for chunk, err := range c.ChatStream(context.Background(), openai.ChatRequest{}) {
		if err != nil {
			t.Fatal(err)
		}
		acc.Add(chunk)
		n++
	}
	check(t, n, 8)
	check(t, srv.request["stream"], true)
	so, _ := srv.request["stream_options"].(map[string]any)
	check(t, so["include_usage"], true)
	checkStreamed(t, acc.Response())
}

func checkStreamed(t *testing.T, resp *openai.ChatResponse) {
	t.Helper()
	want := &openai.ChatResponse{
		ID: "s1", Object: "chat.completion", Created: 5, Model: "m",
		Usage: &openai.Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
		Choices: []openai.Choice{{
			FinishReason: "tool_calls",
			Message: openai.Message{
				Role: openai.RoleAssistant, Content: "Hello", ReasoningContent: "hmm",
				ToolCalls: []openai.ToolCall{
					{ID: "c0", Type: "function", Func: openai.FunctionCall{Name: "run", Arguments: `{"code":1}`}},
					{ID: "c1", Type: "function", Func: openai.FunctionCall{Name: "other", Arguments: "{}"}},
				},
			},
		}},
	}
	if !reflect.DeepEqual(resp, want) {
		t.Errorf("got %+v, want %+v", resp, want)
	}
}

func TestChatStreamErrors(t *testing.T) {
	ctx := context.Background()

	// An error event.
	srv := streamServer(t, "data: {\"error\":{\"message\":\"out of memory\",\"code\":\"oom\"}}\n\n")
	c := newClient(t, srv.URL+"/v1", openai.WithModel("m"))
	err := lastError(c.ChatStream(ctx, openai.ChatRequest{}))
	var ae *openai.APIError
	if !errors.As(err, &ae) || ae.Code != "oom" || ae.Message != "out of memory" {
		t.Errorf("unexpected error: %v", err)
	}

	// Invalid JSON.
	srv = streamServer(t, "data: {\n\n")
	c = newClient(t, srv.URL+"/v1", openai.WithModel("m"))
	if err := lastError(c.ChatStream(ctx, openai.ChatRequest{})); err == nil || !strings.Contains(err.Error(), "decoding") {
		t.Errorf("unexpected error: %v", err)
	}

	// An HTTP error.
	srv = newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	})
	c = newClient(t, srv.URL+"/v1", openai.WithModel("m"))
	err = lastError(c.ChatStream(ctx, openai.ChatRequest{}))
	if !errors.As(err, &ae) || ae.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unexpected error: %v", err)
	}

	// An oversized event.
	srv = streamServer(t, "data: "+strings.Repeat("x", 100)+"\n\n")
	c = newClient(t, srv.URL+"/v1", openai.WithModel("m"), openai.WithMaxResponseBytes(50))
	if err := lastError(c.ChatStream(ctx, openai.ChatRequest{})); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("unexpected error: %v", err)
	}

	// A stream that ends without [DONE] ends the sequence without error.
	srv = streamServer(t, `data: {"choices":[{"index":0,"delta":{"content":"x"}}]}`)
	c = newClient(t, srv.URL+"/v1", openai.WithModel("m"))
	if err := lastError(c.ChatStream(ctx, openai.ChatRequest{})); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// A timeout while waiting for the stream.
	release := make(chan struct{})
	defer close(release)
	srv = newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": keep-alive\n\n")
		w.(http.Flusher).Flush()
		<-release
	})
	c = newClient(t, srv.URL+"/v1", openai.WithModel("m"), openai.WithTimeout(100*time.Millisecond))
	if err := lastError(c.ChatStream(ctx, openai.ChatRequest{})); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("unexpected error: %v", err)
	}
}

// lastError returns the last error, if any, returned by seq.
func lastError(seq func(func(*openai.ChatChunk, error) bool)) error {
	var last error
	for _, err := range seq {
		if err != nil {
			last = err
		}
	}
	return last
}
