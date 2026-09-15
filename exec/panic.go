package exec

import (
	"fmt"
	"log"
	"runtime/debug"
)

// PanicError retains recovery diagnostics privately. It deliberately does not
// unwrap the panic value: panic(response.Error) is not an intentional response.
// Cause and Stack are for operator diagnostics, never protocol serialization.
type PanicError struct {
	operation string
	cause     any
	stack     []byte
}

func (e *PanicError) Error() string { return e.operation + " panicked" }
func (e *PanicError) Cause() any    { return e.cause }
func (e *PanicError) Stack() []byte { return append([]byte(nil), e.stack...) }
func (e *PanicError) Format(s fmt.State, verb rune) {
	if verb == 'v' && s.Flag('+') {
		fmt.Fprintf(s, "%s: %v\n%s", e.Error(), e.cause, e.stack)
		return
	}
	fmt.Fprint(s, e.Error())
}

// NewPanicError must be called at the recovering boundary, before its stack
// unwinds. The standard server log receives diagnostics even without a logger.
func NewPanicError(operation string, cause any) *PanicError {
	err := &PanicError{operation: operation, cause: cause, stack: debug.Stack()}
	log.Printf("%+v", err)
	return err
}
