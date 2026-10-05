package exec

// InputSeed is explicit trusted application input for an internal reader call.
// It is not authentication provenance or a transport/replay parameter.
type InputSeed struct{ input any }

// WithInput supplies an extra canonical typed reader input. Its marked eligible
// values retain legacy cache-before-transform semantics. Callers must not
// mutate the input concurrently with invocation capture.
func WithInput(extra any) *InputSeed { return &InputSeed{input: extra} }

// Value exposes the trusted payload to the native dispatcher. A zero seed is
// invalid; the dispatcher checks canonical identity before any execution.
func (s *InputSeed) Value() any {
	if s == nil {
		return nil
	}
	return s.input
}
