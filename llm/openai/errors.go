// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package openai

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	// maxErrorBody is the maximum number of bytes of an error response
	// that are read.
	maxErrorBody = 64 << 10
	// maxErrorText is the maximum number of bytes of an error response
	// without an error object that are included in an APIError.
	maxErrorText = 1024
)

// APIError is returned for a response whose status is not 2xx, or for an
// error reported in a streamed response.
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

func (e *APIError) Error() string {
	var b strings.Builder
	b.WriteString("openai: ")
	if e.Method != "" {
		fmt.Fprintf(&b, "%v %v: ", e.Method, e.Path)
	}
	if e.StatusCode != 0 {
		fmt.Fprintf(&b, "%v %v", e.StatusCode, http.StatusText(e.StatusCode))
	} else {
		b.WriteString("error")
	}
	if e.Code != "" {
		fmt.Fprintf(&b, " (%v)", e.Code)
	}
	switch {
	case e.Message != "":
		b.WriteString(": " + e.Message)
	case e.Body != "":
		b.WriteString(": " + e.Body)
	}
	return b.String()
}

// errorObject is the error object in an error response, or a streamed
// event.
type errorObject struct {
	Error *struct {
		Message string          `json:"message"`
		Type    string          `json:"type"`
		Param   json.RawMessage `json:"param"`
		Code    json.RawMessage `json:"code"`
	} `json:"error"`
}

// parse returns the error, if any, in buf.
func (eo *errorObject) parse(buf []byte) (*APIError, bool) {
	if err := json.Unmarshal(buf, eo); err != nil || eo.Error == nil {
		return nil, false
	}
	return &APIError{
		Message: eo.Error.Message,
		Type:    eo.Error.Type,
		Param:   rawString(eo.Error.Param),
		Code:    rawString(eo.Error.Code),
	}, true
}

// rawString returns the value of a JSON string, number or null as a string,
// since servers differ as to the type of the error code.
func rawString(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	if v := string(raw); v != "null" {
		return v
	}
	return ""
}

func (c *Client) apiError(resp *http.Response, method, path string) error {
	buf, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	var eo errorObject
	ae, ok := eo.parse(buf)
	if !ok {
		body := strings.TrimSpace(string(buf))
		if len(body) > maxErrorText {
			body = strings.ToValidUTF8(body[:maxErrorText], "") + "..."
		}
		ae = &APIError{Body: body}
	}
	ae.StatusCode, ae.Method, ae.Path = resp.StatusCode, method, path
	return ae
}
