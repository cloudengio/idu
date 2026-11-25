// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmdutil/cmdconfig"
)

// newServer returns a fake server that requires key and records the
// last request.
func newServer(t *testing.T, key string, last *map[string]any) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, `{"error":{"message":"invalid key","code":"invalid_api_key"}}`, http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/models":
			fmt.Fprint(w, `{"data":[{"id":"a"},{"id":"m"}]}`)
		case "/v1/chat/completions":
			buf, _ := io.ReadAll(r.Body)
			*last = nil
			_ = json.Unmarshal(buf, last)
			if (*last)["stream"] == true {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"why\"}}]}\n\n")
				fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello \"}}]}\n\n")
				fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"}}]}\n\ndata: [DONE]\n\n")
				return
			}
			fmt.Fprint(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"hi there","reasoning_content":"because"}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newEnv returns an Env whose configuration, and API key, are read from
// a YAML and a .env file.
func newEnv(t *testing.T, url, key string) *cli.Env {
	t.Helper()
	dir := t.TempDir()
	yamlFile := filepath.Join(dir, "config.yaml")
	envFile := filepath.Join(dir, "secrets.env")
	if err := os.WriteFile(yamlFile, fmt.Appendf(nil, "llm:\n  url: %v/v1\n  model: m\n  api_key_env: IDU_LLM_TEST_KEY\n", url), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(envFile, []byte("IDU_LLM_TEST_KEY="+key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var ff cmdconfig.FilesFlag
	if err := ff.Set(yamlFile + "," + envFile); err != nil {
		t.Fatal(err)
	}
	cfg, loc, err := iduconfig.Load(context.Background(), &ff)
	if err != nil {
		t.Fatal(err)
	}
	vars, err := iduconfig.NewEnvironment(loc)
	if err != nil {
		t.Fatal(err)
	}
	env := cli.NewEnv()
	env.Config = cfg
	env.Environment = vars
	return env
}

func newCommands(env *cli.Env) (*Commands, *bytes.Buffer, *bytes.Buffer) {
	var out, errOut bytes.Buffer
	c := New(env)
	c.out, c.errOut, c.in = &out, &errOut, strings.NewReader("from stdin")
	return c, &out, &errOut
}

func TestCommands(t *testing.T) {
	ctx := context.Background()
	var last map[string]any
	srv := newServer(t, "sekrit", &last)
	c, out, errOut := newCommands(newEnv(t, srv.URL, "sekrit"))

	if err := c.Models(ctx, &ModelsFlags{}, nil); err != nil {
		t.Fatal(err)
	}
	check(t, out.String(), "a\nm (default)\n")

	out.Reset()
	if err := c.Chat(ctx, &ChatFlags{Stream: true, Reasoning: true, System: "be brief"}, []string{"say", "hello"}); err != nil {
		t.Fatal(err)
	}
	check(t, out.String(), "hello world\n")
	check(t, errOut.String(), "why")
	msgs, _ := last["messages"].([]any)
	if len(msgs) != 2 || last["model"] != "m" {
		t.Errorf("unexpected request: %v", last)
	} else if m, _ := msgs[1].(map[string]any); m["content"] != "say hello" {
		t.Errorf("unexpected request: %v", last)
	}

	out.Reset()
	errOut.Reset()
	if err := c.Chat(ctx, &ChatFlags{Model: "a"}, []string{"-"}); err != nil {
		t.Fatal(err)
	}
	check(t, out.String(), "hi there\n")
	if errOut.Len() != 0 {
		t.Errorf("unexpected reasoning: %q", errOut.String())
	}
	if msgs, _ := last["messages"].([]any); last["model"] != "a" || len(msgs) != 1 || msgs[0].(map[string]any)["content"] != "from stdin" {
		t.Errorf("unexpected request: %v", last)
	}

}

func TestCommandErrors(t *testing.T) {
	ctx := context.Background()
	var last map[string]any
	srv := newServer(t, "sekrit", &last)

	// The wrong key.
	c, _, _ := newCommands(newEnv(t, srv.URL, "wrong"))
	err := c.Models(ctx, &ModelsFlags{}, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid key") || strings.Contains(err.Error(), "wrong") {
		t.Errorf("unexpected error: %v", err)
	}

	// No llm configuration.
	c, _, _ = newCommands(cli.NewEnv())
	if err := c.Models(ctx, &ModelsFlags{}, nil); err == nil || !strings.Contains(err.Error(), "llm.url") {
		t.Errorf("unexpected error: %v", err)
	}
}

func check[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
