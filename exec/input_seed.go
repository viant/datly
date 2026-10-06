package exec

// InputSeed carries trusted, exact typed reader input. It is neither transport
// replay nor verified credential provenance. The caller must not mutate Input
// during capture. Runtime admission and selection are enforced before binding.
type InputSeed struct {
	Input any `json:"-"`
}

// WithInput explicitly opts an internal caller into marked reader-input reuse.
// No input is accepted through HTTP/MCP deserialization.
func WithInput(input any) *InputSeed { return &InputSeed{Input: input} }

// Value exposes the trusted payload to the native dispatcher. A zero seed is
// invalid; the dispatcher checks canonical identity before any execution.
func (s *InputSeed) Value() any {
	if s == nil {
		return nil
	}
	return s.Input
}
