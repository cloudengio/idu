// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package llm provides the llm commands, which check access to the OpenAI
// compatible API configured by the llm section of idu's configuration, by
// listing its models and sending it a prompt. The API key is read from the
// environment variable named by llm.api_key_env, which may be set in a
// .env configuration file, see iduconfig.Environment.
package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/llm/openai"
)

// NewClient returns a client for the API configured by cfg, with the API
// key, if any, read from env.
func NewClient(cfg iduconfig.LLM, env *iduconfig.Environment) (*openai.Client, error) {
	if !cfg.Configured() {
		return nil, errors.New("no llm service is configured, set llm.url in the configuration")
	}
	opts := []openai.Option{
		openai.WithModel(cfg.Model),
		openai.WithTimeout(cfg.TimeoutOrDefault()),
		openai.WithAllowInsecure(cfg.AllowInsecure),
	}
	if key := env.Get(cfg.APIKeyEnvOrDefault()); key != "" {
		opts = append(opts, openai.WithAPIKey(key))
	}
	if cfg.AuthHeader != "" {
		opts = append(opts, openai.WithAuthHeader(cfg.AuthHeader))
	}
	return openai.New(cfg.URL, opts...)
}

// ModelsFlags are the flags for the llm models command.
type ModelsFlags struct{}

// ChatFlags are the flags for the llm chat command.
type ChatFlags struct {
	Model     string `subcmd:"model,,'the model to use, defaults to llm.model in the configuration'"`
	System    string `subcmd:"system,,a system prompt"`
	MaxTokens int    `subcmd:"max-tokens,0,'the maximum number of tokens to generate, 0 for the default of the server'"`
	Stream    bool   `subcmd:"stream,true,stream the response as it is generated"`
	Reasoning bool   `subcmd:"reasoning,false,'display the reasoning of the model on stderr, if the server returns it separately'"`
}

// Commands implements the llm commands.
type Commands struct {
	env    *cli.Env
	out    io.Writer
	errOut io.Writer
	in     io.Reader
}

// New returns the Commands for env.
func New(env *cli.Env) *Commands {
	return &Commands{env: env, out: os.Stdout, errOut: os.Stderr, in: os.Stdin}
}

func (c *Commands) client() (*openai.Client, error) {
	return NewClient(c.env.Config.LLM, c.env.Environment)
}

// Models implements the llm models command.
func (c *Commands) Models(ctx context.Context, _ any, _ []string) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	models, err := client.Models(ctx)
	if err != nil {
		return err
	}
	for _, m := range models {
		marker := ""
		if m.ID == client.Model() {
			marker = " (default)"
		}
		fmt.Fprintf(c.out, "%v%v\n", m.ID, marker)
	}
	return nil
}

// Chat implements the llm chat command, values must be a *ChatFlags. The
// prompt is the arguments, or stdin if the only argument is -.
func (c *Commands) Chat(ctx context.Context, values any, args []string) error {
	fl := values.(*ChatFlags)
	prompt, err := c.prompt(args)
	if err != nil {
		return err
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	req := openai.ChatRequest{Model: fl.Model, MaxTokens: fl.MaxTokens}
	if fl.System != "" {
		req.Messages = append(req.Messages, openai.Message{Role: openai.RoleSystem, Content: fl.System})
	}
	req.Messages = append(req.Messages, openai.Message{Role: openai.RoleUser, Content: prompt})
	if fl.Stream {
		return c.chatStream(ctx, client, req, fl.Reasoning)
	}
	return c.chat(ctx, client, req, fl.Reasoning)
}

func (c *Commands) prompt(args []string) (string, error) {
	if len(args) == 1 && args[0] == "-" {
		buf, err := io.ReadAll(c.in)
		return string(buf), err
	}
	return strings.Join(args, " "), nil
}

func (c *Commands) chat(ctx context.Context, client *openai.Client, req openai.ChatRequest, reasoning bool) error {
	resp, err := client.Chat(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Choices) == 0 {
		return errors.New("the response contains no choices")
	}
	msg := resp.Choices[0].Message
	if reasoning && msg.ReasoningContent != "" {
		fmt.Fprintln(c.errOut, msg.ReasoningContent)
	}
	fmt.Fprintln(c.out, msg.Content)
	return nil
}

func (c *Commands) chatStream(ctx context.Context, client *openai.Client, req openai.ChatRequest, reasoning bool) error {
	for chunk, err := range client.ChatStream(ctx, req) {
		if err != nil {
			return err
		}
		for _, ch := range chunk.Choices {
			if ch.Index != 0 {
				continue
			}
			if reasoning {
				fmt.Fprint(c.errOut, ch.Delta.ReasoningContent)
			}
			fmt.Fprint(c.out, ch.Delta.Content)
		}
	}
	fmt.Fprintln(c.out)
	return nil
}
