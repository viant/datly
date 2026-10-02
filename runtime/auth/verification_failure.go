package auth

import "fmt"

// VerificationFailure preserves the verifier cause without exposing credentials
// in logging, formatting or JSON. Retention requires explicit trusted config.
type VerificationFailure struct {
	cause      error
	credential string
	retained   bool
}

func (e *VerificationFailure) Error() string { return fmt.Sprintf("verify JWT: %v", e.cause) }
func (e *VerificationFailure) Unwrap() error { return e.cause }

// FailedCredential is for an application explicitly reproducing a legacy
// response contract. Default configuration returns no retained credential.
func (e *VerificationFailure) FailedCredential() (string, bool) {
	if e == nil {
		return "", false
	}
	return e.credential, e.retained
}

// Format also protects %#v, which otherwise prints private struct fields.
func (e *VerificationFailure) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, e.Error()) }
