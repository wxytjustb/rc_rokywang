package api

import (
	"bytes"
	"encoding/json"
)

// canonicalEqual compares two JSON documents by value rather than by byte
// layout, so {"a":1,"b":2} and {"b":2,"a":1} are treated as the same
// payload when deciding whether a repeated (source_system,
// source_request_id) submission is a true idempotent duplicate.
// json.Marshal of a decoded map sorts keys, which is what makes this work.
func canonicalEqual(a, b json.RawMessage) bool {
	ca, err := canonicalize(a)
	if err != nil {
		return bytes.Equal(a, b)
	}
	cb, err := canonicalize(b)
	if err != nil {
		return bytes.Equal(a, b)
	}
	return bytes.Equal(ca, cb)
}

func canonicalize(raw json.RawMessage) ([]byte, error) {
	var v interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
