package logging

import (
	"context"
	xexec "github.com/viant/xdatly/exec"
)

// State is privately embedded by the public recorder. The unexported method
// permits internal cross-package emission without a public forwarding API.
type State struct{ sink *Sink }

func NewState(sink *Sink) *State { return &State{sink: sink} }
func (s *State) loggingSink() *Sink {
	if s == nil {
		return nil
	}
	return s.sink
}

type owner interface{ loggingSink() *Sink }

// ShareState borrows the same immutable sink without introducing another owner.
func ShareState(o owner) *State {
	if o == nil {
		return nil
	}
	return NewState(o.loggingSink())
}

func Enabled(o owner) bool { return o != nil && o.loggingSink().Enabled() }
func LogHTTP(o owner, ctx context.Context, e *xexec.Context) {
	if o != nil {
		o.loggingSink().HTTP(ctx, e)
	}
}
