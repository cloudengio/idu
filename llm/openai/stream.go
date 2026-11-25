// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package openai

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
)

// ChatStream returns the completion for req as a sequence of chunks, read
// from a stream of server-sent events. The sequence ends after the last
// chunk or the first error; the stream is closed if the loop over it is
// ended early. Use an Accumulator to assemble the chunks into a
// ChatResponse.
func (c *Client) ChatStream(ctx context.Context, req ChatRequest) iter.Seq2[*ChatChunk, error] {
	return func(yield func(*ChatChunk, error) bool) {
		ctx, cancel := c.withTimeout(ctx)
		defer cancel()
		req.Stream = true
		if req.StreamOptions == nil {
			req.StreamOptions = &StreamOptions{IncludeUsage: true}
		}
		resp, err := c.post(ctx, "/chat/completions", req)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()
		events := &eventReader{rd: bufio.NewReader(resp.Body), max: c.opts.maxResponse}
		for {
			data, err := events.next()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				if ctx.Err() != nil {
					err = ctx.Err()
				}
				yield(nil, fmt.Errorf("openai: reading stream: %w", err))
				return
			}
			if bytes.Equal(data, []byte("[DONE]")) {
				return
			}
			var eo errorObject
			if ae, ok := eo.parse(data); ok {
				ae.Method, ae.Path = "POST", "/chat/completions"
				yield(nil, ae)
				return
			}
			var chunk ChatChunk
			if err := json.Unmarshal(data, &chunk); err != nil {
				yield(nil, fmt.Errorf("openai: decoding stream: %w", err))
				return
			}
			if !yield(&chunk, nil) {
				return
			}
		}
	}
}

// eventReader reads the data of server-sent events.
type eventReader struct {
	rd  *bufio.Reader
	max int64
}

// next returns the data of the next event that has any, joining the data
// lines of a multi-line event with newlines. Comments, which servers send
// to keep a connection alive during a long prefill, and fields other than
// data are ignored. It returns io.EOF at the end of the stream.
func (er *eventReader) next() ([]byte, error) {
	var data []byte
	have := false
	for {
		line, err := er.line()
		if err != nil {
			if errors.Is(err, io.EOF) && have {
				return data, nil
			}
			return nil, err
		}
		if len(line) == 0 {
			if have {
				return data, nil
			}
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		if len(field) == 0 || string(field) != "data" {
			continue
		}
		value = bytes.TrimPrefix(value, []byte(" "))
		if have {
			data = append(data, '\n')
		}
		data = append(data, value...)
		have = true
		if int64(len(data)) > er.max {
			return nil, fmt.Errorf("event exceeds %v bytes", er.max)
		}
	}
}

// line returns the next line, without its line ending.
func (er *eventReader) line() ([]byte, error) {
	var line []byte
	for {
		frag, isPrefix, err := er.rd.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return line, nil
			}
			return nil, err
		}
		line = append(line, frag...)
		if int64(len(line)) > er.max {
			return nil, fmt.Errorf("line exceeds %v bytes", er.max)
		}
		if !isPrefix {
			return line, nil
		}
	}
}

// Accumulator assembles the chunks of a streamed response into a
// ChatResponse: the content, reasoning and the fragments of the arguments
// of each tool call are concatenated.
type Accumulator struct {
	resp    ChatResponse
	choices map[int]*Choice
	calls   map[int]map[int]*ToolCall
}

// Add adds chunk to the response.
func (a *Accumulator) Add(chunk *ChatChunk) {
	if a.choices == nil {
		a.choices = map[int]*Choice{}
		a.calls = map[int]map[int]*ToolCall{}
	}
	a.resp.ID = cmp.Or(a.resp.ID, chunk.ID)
	a.resp.Model = cmp.Or(a.resp.Model, chunk.Model)
	if a.resp.Created == 0 {
		a.resp.Created = chunk.Created
	}
	if chunk.Usage != nil {
		a.resp.Usage = chunk.Usage
	}
	for _, cc := range chunk.Choices {
		ch, ok := a.choices[cc.Index]
		if !ok {
			ch = &Choice{Index: cc.Index}
			a.choices[cc.Index] = ch
		}
		d := cc.Delta
		if d.Role != "" {
			ch.Message.Role = d.Role
		}
		ch.Message.Content += d.Content
		ch.Message.ReasoningContent += d.ReasoningContent
		if cc.FinishReason != "" {
			ch.FinishReason = cc.FinishReason
		}
		for i, tc := range d.ToolCalls {
			a.addToolCall(cc.Index, i, tc)
		}
	}
}

func (a *Accumulator) addToolCall(choice, pos int, tc ToolCall) {
	calls := a.calls[choice]
	if calls == nil {
		calls = map[int]*ToolCall{}
		a.calls[choice] = calls
	}
	idx := pos
	if tc.Index != nil {
		idx = *tc.Index
	}
	call, ok := calls[idx]
	if !ok {
		call = &ToolCall{}
		calls[idx] = call
	}
	call.ID = cmp.Or(call.ID, tc.ID)
	call.Type = cmp.Or(call.Type, tc.Type)
	call.Func.Name = cmp.Or(call.Func.Name, tc.Func.Name)
	call.Func.Arguments += tc.Func.Arguments
}

// Response returns the response assembled so far.
func (a *Accumulator) Response() *ChatResponse {
	resp := a.resp
	resp.Object = "chat.completion"
	resp.Choices = nil
	for _, i := range slices.Sorted(maps.Keys(a.choices)) {
		c := *a.choices[i]
		calls := a.calls[i]
		for _, j := range slices.Sorted(maps.Keys(calls)) {
			call := *calls[j]
			call.Index = nil
			c.Message.ToolCalls = append(c.Message.ToolCalls, call)
		}
		resp.Choices = append(resp.Choices, c)
	}
	return &resp
}
