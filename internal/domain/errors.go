package domain

import "errors"

// Sentinel errors. Callers check them with errors.Is, so adapters can wrap
// them with context (fmt.Errorf("...: %w", err)) without losing the meaning.
var (
	ErrNotFound       = errors.New("not found")
	ErrValidation     = errors.New("validation failed")
	ErrAlreadySettled = errors.New("bet already settled")
)

// ValidationError carries the field that failed. It unwraps to ErrValidation,
// so both errors.Is(err, ErrValidation) and errors.As(err, &ve) work.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Reason }

func (e *ValidationError) Unwrap() error { return ErrValidation }
