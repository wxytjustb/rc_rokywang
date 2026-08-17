package provider

import "fmt"

// ValidationError lists every payload field missing or wrong-typed, so a
// caller gets one useful 422 response instead of playing whack-a-mole with
// one field at a time.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("payload validation failed: %v", e.Problems)
}
