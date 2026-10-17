package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/bindly"
	legacy "github.com/viant/xdatly/handler/response"
)

type serverCycle struct{}

func (e *serverCycle) Error() string { panic("diagnostic must not be read") }
func (e *serverCycle) Unwrap() error { return e }

func TestMaskedComponentServerErrorCompleteCauseIdentity(t *testing.T) {
	sqlCause := fmt.Errorf("private SQL: %w", errors.New("private credential"))
	retained := &bindly.BindingError{Name: "Current", Kind: "component", Cause: sqlCause}
	nominal := &legacy.Error{Code: 500, Message: "private nominal", Object: map[string]any{"secret": "private object"}, Err: retained}
	owner := &bindly.BindingError{Name: "Access", Kind: "component", Cause: nominal}
	for _, tc := range []struct {
		name  string
		cause error
		want  bool
	}{
		{"owned", owner, true}, {"duplicate", errors.Join(owner, sqlCause), true},
		{"distinct same message", errors.Join(owner, fmt.Errorf("private SQL: %w", errors.New("private credential"))), false},
		{"extra cleanup", errors.Join(owner, sqlCause, errors.New("cleanup")), false},
		{"cancellation", errors.Join(owner, context.Canceled), false},
		{"unowned", nominal, false}, {"cycle", errors.Join(owner, &serverCycle{}), false},
		{"conflict", &bindly.BindingError{Name: "Access", Kind: "component", Code: 403, Cause: nominal}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries, ok := maskedComponentServerError(tc.cause, 500, "Records")
			if ok != tc.want {
				t.Fatalf("matched=%t want=%t", ok, tc.want)
			}
			if !ok {
				return
			}
			if len(entries) != 1 || entries[0].Code != 500 || entries[0].Parameter != "Access" || entries[0].View != "Records" || entries[0].Message != "Internal Server Error" || entries[0].Err != nominal {
				t.Fatal("masked ownership lost")
			}
			wire, err := json.Marshal(entries)
			if err != nil || strings.Contains(string(wire), "private") || strings.Contains(string(wire), "secret") {
				t.Fatal("private cause escaped")
			}
		})
	}
}

type codedServerGroup struct{ children []error }

func (*codedServerGroup) Error() string     { return "private" }
func (*codedServerGroup) StatusCode() int   { return 503 }
func (e *codedServerGroup) Unwrap() []error { return e.children }

type syntheticServerWrapper struct{ cause error }

func (*syntheticServerWrapper) Error() string   { return "private" }
func (*syntheticServerWrapper) As(any) bool     { return true }
func (e *syntheticServerWrapper) Unwrap() error { return e.cause }

type changingServerWrapper struct {
	calls         int
	first, second error
}

func (*changingServerWrapper) Error() string { return "private" }
func (e *changingServerWrapper) Unwrap() error {
	e.calls++
	if e.calls == 1 {
		return e.first
	}
	return e.second
}

func TestComponentServerProjectionRejectsHiddenAuthorityAndChangedEdges(t *testing.T) {
	for _, mode := range []string{"group status", "synthetic As", "changed edge"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("private retained")
			nominal := &legacy.Error{Code: 500, Err: cause}
			owner := &bindly.BindingError{Name: "Access", Kind: "component", Cause: nominal}
			var failure error
			switch mode {
			case "group status":
				failure = &codedServerGroup{[]error{owner, cause}}
			case "synthetic As":
				wrapper := &syntheticServerWrapper{cause}
				nominal.Err = wrapper
				failure = errors.Join(owner, wrapper)
			case "changed edge":
				wrapper := &changingServerWrapper{first: cause, second: errors.New("private distinct")}
				nominal.Err = wrapper
				failure = errors.Join(owner, wrapper)
			}
			p := bindingPlan(t)
			p.errorEnvelope.readerView = "Records"
			body, applicable, err := p.ErrorEnvelopeWithCause("Internal Server Error", 500, failure)
			if err != nil || !applicable {
				t.Fatal("declared output unavailable", err)
			}
			if body.(*errorEnvelopeValue).Errors != nil {
				t.Fatal("unsupported graph granted ownership")
			}
		})
	}
}

type panickingSingleCause struct{}

func (*panickingSingleCause) Error() string { return "private" }
func (*panickingSingleCause) Unwrap() error { panic("private unwrap") }

type panickingGroupCause struct{}

func (*panickingGroupCause) Error() string   { return "private" }
func (*panickingGroupCause) Unwrap() []error { panic("private group unwrap") }

func TestDeclaredReaderErrorProjectionUnwrapPanicFallback(t *testing.T) {
	for _, cause := range []error{&panickingSingleCause{}, &panickingGroupCause{}} {
		p := bindingPlan(t)
		p.errorEnvelope.readerView = "Records"
		body, applicable, err := p.ErrorEnvelopeWithCause("Internal Server Error", 500, cause)
		if err != nil || !applicable || body.(*errorEnvelopeValue).Errors != nil {
			t.Fatal("plain declared fallback lost", err)
		}
	}
}
