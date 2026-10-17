package output

import (
	"errors"
	"fmt"
	"github.com/viant/bindly"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
	"testing"
)

func TestReaderBindingSourceForwarding(t *testing.T) {
	child := nominalBusinessPlan(t, nominalBusinessComponent())
	proxy := bindingPlan(t)
	leaf := bindingLeaf("Fields", 400)
	marked := child.PreserveReaderBindingSource(leaf)
	if marked == leaf || !errors.Is(marked, leaf) {
		t.Fatal("source/cause not retained")
	}
	forwarded := fmt.Errorf("forwarded: %w", fmt.Errorf("second hop: %w", marked))
	out := projected(t, proxy, forwarded, 400)
	details := out.Errors.([]*legacy.Error)
	if details[0].View != "AuthoritativeRecords" {
		t.Fatalf("source %q", details[0].View)
	}
	details[0].View = "changed"
	if projected(t, proxy, forwarded, 400).Errors.([]*legacy.Error)[0].View != "AuthoritativeRecords" {
		t.Fatal("shared projected output")
	}
	parentComponent := nominalBusinessComponent()
	parentComponent.RootView.Name = "Parent"
	parent := nominalBusinessPlan(t, parentComponent)
	if projected(t, parent, forwarded, 400).Errors.([]*legacy.Error)[0].View != "Parent" {
		t.Fatal("parent authority lost")
	}
	for _, err := range []error{errors.Join(marked, errors.New("other")), &bindly.BindingError{Name: "Child", Kind: "component", Cause: marked}, &modern.Error{Code: 400, Cause: marked}} {
		if sourceReaderView(err) != "" {
			t.Fatal("nontransparent source forwarding")
		}
	}
	for _, err := range []error{errors.New("sql failure"), &legacy.Error{Code: 403, Message: "denied"}, &modern.Error{Code: 400, Payload: "existing"}, errors.Join(leaf, errors.New("other")), nominalPrivateCause{}} {
		if got := child.PreserveReaderBindingSource(err); got != err {
			t.Fatal("ineligible changed")
		}
	}
	if sourceReaderView(leaf) != "" {
		t.Fatal("unowned source inferred")
	}
}

type cyclicBindingSourceError struct{}

func (*cyclicBindingSourceError) Error() string   { return "cycle" }
func (e *cyclicBindingSourceError) Unwrap() error { return e }
func TestReaderBindingSourceRejectsCycle(t *testing.T) {
	p := nominalBusinessPlan(t, nominalBusinessComponent())
	cause := &cyclicBindingSourceError{}
	if p.PreserveReaderBindingSource(cause) != cause {
		t.Fatal("cycle changed")
	}
}

type changingBindingSourceError struct {
	calls int
	first error
}

func (e *changingBindingSourceError) Error() string { return "changing" }
func (e *changingBindingSourceError) Unwrap() error {
	e.calls++
	if e.calls == 1 {
		return e.first
	}
	return e
}
func TestReaderBindingSourceParsesOnce(t *testing.T) {
	p := nominalBusinessPlan(t, nominalBusinessComponent())
	cause := &changingBindingSourceError{first: bindingLeaf("Fields", 400)}
	p.PreserveReaderBindingSource(cause)
	if cause.calls != 1 {
		t.Fatalf("cause traversed %d times", cause.calls)
	}
}
