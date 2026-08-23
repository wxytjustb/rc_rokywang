package notification

import (
	"bytes"
	"encoding/json"
)

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
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
