package exec

import "fmt"

// IndependentChildTransactionError reports a rejected trusted orchestration
// policy before input binding or child mutation. Reason never contains inputs.
type IndependentChildTransactionError struct{ Reason string }

func (e *IndependentChildTransactionError) Error() string {
	return fmt.Sprintf("independent child transactions require a source-less custom root: %s", e.Reason)
}
