package writer

import (
	"context"
	"errors"
	rhandler "github.com/viant/datly/runtime/handler"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type aggregateValidationProbe struct {
	calls  int
	native int
}

func (*aggregateValidationProbe) Init(context.Context, *sqPlainParent, h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	return nil
}
func (p *aggregateValidationProbe) ValidateInput(_ context.Context, _ *sqPlainInput, _ *sqPlainOutput, report h.ValidationReport) error {
	p.calls++
	p.native = len(report.SchemaViolations())
	report.Add(h.Violation{Message: "business violation", Check: "business"})
	return nil
}

type aggregateCapabilities struct {
	fakeCapabilities
	operational         error
	starts, allocations int
}

func (c *aggregateCapabilities) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.FrameworkValidatorKey || key == h.TransactionStarterKey || key == h.SequencerKey {
		return c, true, nil
	}
	return c.fakeCapabilities.Lookup(ctx, key)
}
func (c *aggregateCapabilities) Validate(context.Context, any, ...any) (*h.Validation, error) {
	if c.operational != nil {
		return nil, c.operational
	}
	return &h.Validation{Violations: []*h.Violation{{Message: "schema violation", Check: "schema"}}}, nil
}
func (c *aggregateCapabilities) Start(context.Context) error { c.starts++; return nil }
func (c *aggregateCapabilities) Allocate(context.Context, string, any, string) error {
	c.allocations++
	return nil
}

func TestAggregateValidationRunsAfterSchemaViolationsAndForEmptyInput(t *testing.T) {
	for _, tc := range []struct {
		name               string
		empty, operational bool
	}{{"schema and business", false, false}, {"empty input", true, false}, {"operational validator error", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			handler, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			handler.metadata.Root.HookType = reflect.TypeFor[aggregateValidationProbe]()
			if err = validateAggregateHooks(handler.metadata.Root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput]()); err != nil {
				t.Fatal(err)
			}
			input := &sqPlainInput{}
			if !tc.empty {
				input.Rows = []*sqPlainParent{{Name: ptr("new"), Has: &sqPlainParentHas{Name: true}}}
			}
			snapshot, err := handler.CaptureInput(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			caps := &aggregateCapabilities{}
			// A validator-returned error wrapping Validation is still operational.
			if tc.operational {
				caps.operational = &h.Validation{Failed: true}
			}
			_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: caps})
			probe := snapshot.(*Program).hook.Interface().(*aggregateValidationProbe)
			if tc.operational {
				if probe.calls != 0 || !errors.Is(err, caps.operational) {
					t.Fatal("operational error reached business validation or lost cause")
				}
			} else {
				var validation *h.Validation
				want := 2
				if tc.empty {
					want = 1
				}
				if probe.calls != 1 || !errors.As(err, &validation) || len(validation.Violations) != want {
					t.Fatalf("calls=%d error=%v", probe.calls, err)
				}
				if !tc.empty && (probe.native != 1 || validation.Violations[0].Check != "schema" || validation.Violations[1].Check != "business") {
					t.Fatal("schema/business evidence order changed")
				}
			}
			if caps.starts != 0 || caps.allocations != 0 || caps.inserts+caps.updates+caps.deletes != 0 {
				t.Fatal("invalid input reached transaction, allocation or DML")
			}
		})
	}
}

type passingAggregateProbe struct{ calls int }

func (*passingAggregateProbe) Init(context.Context, *sqPlainParent, h.LifecycleContext[sqPlainParent, h.NoParent, sqPlainOutput]) error {
	return nil
}
func (p *passingAggregateProbe) ValidateInput(context.Context, *sqPlainInput, *sqPlainOutput, h.ValidationReport) error {
	p.calls++
	return nil
}

type finalValidationCapabilities struct {
	aggregateCapabilities
	passes    int
	failFinal bool
}

func (c *finalValidationCapabilities) Lookup(ctx context.Context, key h.ValueKey) (any, bool, error) {
	if key == h.FrameworkValidatorKey {
		return c, true, nil
	}
	return c.aggregateCapabilities.Lookup(ctx, key)
}
func (c *finalValidationCapabilities) Validate(context.Context, any, ...any) (*h.Validation, error) {
	c.passes++
	if c.passes == 2 && c.failFinal {
		return &h.Validation{Violations: []*h.Violation{{Message: "final generated value invalid", Check: "required"}}}, nil
	}
	return &h.Validation{}, nil
}
func TestAggregateValidationCannotBypassFinalProducedValueChecks(t *testing.T) {
	for _, failFinal := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "final violation"}[failFinal], func(t *testing.T) {
			handler, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			handler.metadata.Root.HookType = reflect.TypeFor[passingAggregateProbe]()
			input := &sqPlainInput{Rows: []*sqPlainParent{{Name: ptr("new"), Has: &sqPlainParentHas{Name: true}}}}
			snapshot, err := handler.CaptureInput(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			caps := &finalValidationCapabilities{failFinal: failFinal}
			_, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: caps})
			if (err != nil) != failFinal {
				t.Fatalf("error=%v", err)
			}
			if caps.passes != 2 || snapshot.(*Program).hook.Interface().(*passingAggregateProbe).calls != 1 {
				t.Fatal("initial/final validation or once-per-input callback changed")
			}
			if failFinal && caps.inserts+caps.updates+caps.deletes != 0 {
				t.Fatal("final violation reached DML")
			}
			if !failFinal && caps.inserts != 1 {
				t.Fatal("valid input did not reach normal insertion")
			}
		})
	}
}
