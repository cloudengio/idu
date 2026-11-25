// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"fmt"
	"net/url"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultLLMAPIKeyEnv is the environment variable that holds the API
	// key for the LLM service when api_key_env is not set.
	DefaultLLMAPIKeyEnv = "IDU_LLM_API_KEY" //nolint:gosec // G101: the name of a variable, not a credential

	// DefaultLLMTimeout is the time allowed for a request to the LLM
	// service, including a streamed response, when timeout is not set.
	DefaultLLMTimeout = 5 * time.Minute
)

// LLM configures access to an OpenAI compatible API, eg. one served by
// oMLX, llama-server or a hosted service. It deliberately has no field for
// the API key: the key is read from the environment variable named by
// APIKeyEnv, which may be set in the process environment or in one of the
// .env files that the configuration is loaded with, see Environment, so
// that it is never stored in, or displayed as part of, the YAML
// configuration.
type LLM struct {
	URL           string        `yaml:"url,omitempty" doc:"the base URL of the OpenAI compatible API, including its version prefix, eg. http://127.0.0.1:8000/v1"`
	Model         string        `yaml:"model,omitempty" doc:"the model used by requests that do not specify one"`
	APIKeyEnv     string        `yaml:"api_key_env,omitempty" doc:"the name of the environment variable that holds the API key, it is looked up in the process environment and then in the .env configuration files, defaults to IDU_LLM_API_KEY; no key is sent if it is not set. The key itself cannot be specified in the YAML configuration."`
	AuthHeader    string        `yaml:"auth_header,omitempty" doc:"the HTTP header that carries the API key, defaults to Authorization, whose value is 'Bearer <key>'; any other header, eg. api-key or x-api-key, carries the key as is"`
	Timeout       time.Duration `yaml:"timeout,omitempty" doc:"the time allowed for each request, including a streamed response, defaults to 5m"`
	AllowInsecure bool          `yaml:"allow_insecure,omitempty" doc:"allow the API key to be sent over plain http to a host other than localhost, which is otherwise an error"`
}

var llmYAMLFields = structYAMLFields(LLM{})

// UnmarshalYAML implements yaml.Unmarshaler, rejecting unknown fields, in
// particular an attempt to specify the API key itself.
func (l *LLM) UnmarshalYAML(value *yaml.Node) error {
	if err := validateMappingKeys(value, llmYAMLFields, "config.LLM"); err != nil {
		return err
	}
	type rawLLM LLM
	return value.Decode((*rawLLM)(l))
}

// Configured returns true if the LLM service has been configured, ie. its
// URL is set.
func (l LLM) Configured() bool {
	return l.URL != ""
}

// APIKeyEnvOrDefault returns APIKeyEnv, or DefaultLLMAPIKeyEnv if it is
// not set.
func (l LLM) APIKeyEnvOrDefault() string {
	if l.APIKeyEnv == "" {
		return DefaultLLMAPIKeyEnv
	}
	return l.APIKeyEnv
}

// TimeoutOrDefault returns Timeout, or DefaultLLMTimeout if it is not set.
func (l LLM) TimeoutOrDefault() time.Duration {
	if l.Timeout == 0 {
		return DefaultLLMTimeout
	}
	return l.Timeout
}

func (l LLM) validate() error {
	if l.URL == "" {
		return nil
	}
	u, err := url.Parse(l.URL)
	if err != nil {
		return fmt.Errorf("llm: url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("llm: url %q: the scheme must be http or https", l.URL)
	}
	if u.Host == "" {
		return fmt.Errorf("llm: url %q: no host", l.URL)
	}
	if u.User != nil {
		return fmt.Errorf("llm: url %q: credentials must not be included in the url, use api_key_env", u.Redacted())
	}
	if l.Timeout < 0 {
		return fmt.Errorf("llm: timeout must not be negative (%v)", l.Timeout)
	}
	return nil
}
