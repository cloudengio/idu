// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/cmdconfig"
	"gopkg.in/yaml.v3"
)

func TestLLM(t *testing.T) {
	cfg, err := iduconfig.ParseConfig([]byte(`llm:
  url: http://127.0.0.1:8000/v1
  model: qwen
  api_key_env: MY_KEY
  auth_header: x-api-key
  timeout: 30s
`))
	if err != nil {
		t.Fatal(err)
	}
	l := cfg.LLM
	if !l.Configured() || l.URL != "http://127.0.0.1:8000/v1" || l.Model != "qwen" || l.AuthHeader != "x-api-key" || l.AllowInsecure {
		t.Errorf("unexpected configuration: %+v", l)
	}
	check(t, l.APIKeyEnvOrDefault(), "MY_KEY")
	check(t, l.TimeoutOrDefault(), 30*time.Second)

	var def iduconfig.LLM
	if def.Configured() || def.APIKeyEnvOrDefault() != iduconfig.DefaultLLMAPIKeyEnv || def.TimeoutOrDefault() != iduconfig.DefaultLLMTimeout {
		t.Errorf("unexpected defaults: %+v", def)
	}

	// An unconfigured llm section is not displayed.
	out, err := yaml.Marshal(iduconfig.T{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "llm") {
		t.Errorf("unexpected llm section: %s", out)
	}

}

func TestLLMErrors(t *testing.T) {
	for _, tc := range []struct {
		spec, err string
	}{
		{"llm:\n  url: ftp://host/v1\n", "scheme"},
		{"llm:\n  url: http:///v1\n", "no host"},
		{"llm:\n  url: https://user:secret@host/v1\n", "credentials"},
		{"llm:\n  url: https://host/v1\n  timeout: -1s\n", "negative"},
		// The key cannot be stored in the YAML configuration.
		{"llm:\n  url: https://host/v1\n  api_key: k\n", "field api_key not found"},
	} {
		_, err := iduconfig.ParseConfig([]byte(tc.spec))
		if err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%q: got %v, want an error containing %q", tc.spec, err, tc.err)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("%q: error contains the password: %v", tc.spec, err)
		}
	}
}

func TestEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Chdir(t.TempDir())
	t.Setenv("HOME", home)
	t.Setenv("APPDATA", filepath.Join(home, ".config"))
	t.Setenv(iduconfig.EnvConfig, "")
	t.Setenv(iduconfig.EnvVars, "")
	t.Setenv("IDU_TEST_FROM_PROCESS", "process")

	confDir := filepath.Join(home, ".config", iduconfig.AppName)
	if err := os.MkdirAll(confDir, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(confDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("config.yaml", `llm:
  url: https://llm.example.com/v1
  api_key_env: IDU_TEST_KEY
`)
	write("a.env", "IDU_TEST_KEY=from-a\nIDU_TEST_BASE=base\nIDU_TEST_FROM_PROCESS=file\n")
	write("b.env", "IDU_TEST_KEY=${IDU_TEST_BASE}-secret\n")

	ctx := context.Background()
	cfg, loc, err := iduconfig.Load(ctx, &cmdconfig.FilesFlag{})
	if err != nil {
		t.Fatal(err)
	}
	env, err := iduconfig.NewEnvironment(loc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(env.Files()), 2; got != want {
		t.Fatalf("got %v, want %v: %v", got, want, env.Files())
	}
	for _, tc := range []struct{ name, want string }{
		// The later file takes precedence, and references are expanded.
		{cfg.LLM.APIKeyEnvOrDefault(), "base-secret"},
		// The process environment takes precedence over the files.
		{"IDU_TEST_FROM_PROCESS", "process"},
		{"IDU_TEST_UNDEFINED", ""},
		{"", ""},
	} {
		if got := env.Get(tc.name); got != tc.want {
			t.Errorf("%q: got %q, want %q", tc.name, got, tc.want)
		}
	}

	// The key is not part of the configuration that config show displays,
	// only the name of the variable that holds it.
	var buf bytes.Buffer
	cc := iduconfig.ConfigCommands()
	cc.Output = &buf
	showCtx := cmdutil.ContextWithValue(cmdutil.ContextWithValue(ctx, cfg), loc)
	if err := cc.Show(showCtx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); strings.Contains(out, "secret") || !strings.Contains(out, "IDU_TEST_KEY") || !strings.Contains(out, "b.env") {
		t.Errorf("unexpected configuration: %s", out)
	}

	// A nil Located, or Environment, provides the process environment, or
	// nothing.
	env, err = iduconfig.NewEnvironment(nil)
	if err != nil {
		t.Fatal(err)
	}
	check(t, env.Get("IDU_TEST_FROM_PROCESS"), "process")
	var none *iduconfig.Environment
	if none.Get("IDU_TEST_FROM_PROCESS") != "" || none.Files() != nil {
		t.Errorf("nil Environment is not empty")
	}
}

func check[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
