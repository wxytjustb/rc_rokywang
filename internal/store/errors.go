package store

import "errors"

var (
	// ErrNotFound is returned when a lookup by id or idempotency key finds
	// no row.
	ErrNotFound = errors.New("store: not found")
	// ErrDuplicateSourceRequest is returned by Insert when
	// (source_system, source_request_id) already exists.
	ErrDuplicateSourceRequest = errors.New("store: duplicate source request")
	// ErrNotClaimed is returned when an atomic claim/transition update
	// affects zero rows: the event was already claimed by another worker,
	// already reached a terminal state, or the lease token no longer
	// matches (a newer worker already took over).
	ErrNotClaimed = errors.New("store: event not claimed")
)
