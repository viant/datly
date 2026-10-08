package writer

import (
	"context"
	"errors"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	rcompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

type preparationRuntimeHook struct{}

func (*preparationRuntimeHook) AfterValidateInput(context.Context, *sqPlainInput, *sqPlainOutput) error {
	return nil
}

type preparationRuntimeWrongHook struct{}

func (*preparationRuntimeWrongHook) AfterValidateInput(context.Context, *hookInput, *sqPlainOutput) error {
	return nil
}

type preparationRuntimeVariadicHook struct{}

func (*preparationRuntimeVariadicHook) AfterValidateInput(context.Context, *sqPlainInput, ...*sqPlainOutput) error {
	return nil
}

func TestAfterValidateInputRuntimeDeclarations(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[preparationRuntimeWrongHook](), reflect.TypeFor[preparationRuntimeVariadicHook]()} {
		root := &Record{HookType: typ}
		if err := validateAfterValidateInputHooks(root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput]()); err == nil {
			t.Fatal("malformed hook accepted")
		}
	}
	root := &Record{HookType: reflect.TypeFor[preparationRuntimeHook]()}
	child := &Record{HookType: root.HookType}
	root.Relations = []*Relation{{Field: []int{0}, Child: child}}
	if err := validateAfterValidateInputHooks(root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput]()); err == nil {
		t.Fatal("descendant hook accepted")
	}
	child.HookType = nil
	// The native compiler currently emits direct holders. Fail closed if richer
	// embedded relation metadata ever reaches this narrowly approved boundary.
	root.Relations[0].Field = []int{0, 1}
	if err := validateAfterValidateInputHooks(root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput]()); err == nil || !strings.Contains(err.Error(), "direct canonical") {
		t.Fatalf("embedded metadata admitted: %v", err)
	}
	root.Relations[0].Field = []int{0}
	if err := validateAfterValidateInputHooks(root, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput]()); err != nil {
		t.Fatal(err)
	}
}

type preparationEmbedded struct {
	Name     string
	Children []*sqPlainChild
}
type preparationEmbeddedRow struct{ Details preparationEmbedded }

func TestAfterValidateInputDoesNotMaskEmbeddedScalarSiblings(t *testing.T) {
	record := &Record{Relations: []*Relation{{Field: []int{0, 1}}}}
	row := &preparationEmbeddedRow{Details: preparationEmbedded{Name: "protected"}}
	before, err := preparationScalar(record, reflect.ValueOf(row))
	if err != nil {
		t.Fatal(err)
	}
	row.Details.Name = "illegal"
	after, err := preparationScalar(record, reflect.ValueOf(row))
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("nested relation masked a scalar sibling")
	}
}

type preparationFailureKey struct{}
type preparationFailureProbe struct {
	program *Program
	mode    string
}
type preparationFailureHook struct{}

func (*preparationFailureHook) AfterValidateInput(ctx context.Context, _ *sqPlainInput, _ *sqPlainOutput) error {
	probe := ctx.Value(preparationFailureKey{}).(*preparationFailureProbe)
	frame := probe.program.frames.Rows[0]
	if strings.HasPrefix(probe.mode, "Original") {
		frame.Original = originalPresence{available: false}
	} else {
		*frame.Previous.Elem().FieldByName("Name").Interface().(*string) = "illegal Previous"
	}
	if strings.HasSuffix(probe.mode, "panic") {
		panic("failed evidence fixture")
	}
	return preparationFailedEvidence
}

var preparationFailedEvidence = errors.New("failed evidence fixture")

func TestAfterValidateInputFailedCallbackEvidenceAndRetry(t *testing.T) {
	for _, mode := range []string{"Original error", "Previous error", "Original panic", "Previous panic"} {
		t.Run(mode, func(t *testing.T) {
			native, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			native.metadata.Root.HookType = reflect.TypeFor[preparationFailureHook]()
			input := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}}, CurrentRows: []*sqPlainParent{{ID: ptr(1), Name: ptr("stored")}}}
			program, err := native.program(input)
			if err != nil {
				t.Fatal(err)
			}
			if err := program.indexRecordCurrent(reflect.ValueOf(input).Elem(), program.metadata.Root); err != nil {
				t.Fatal(err)
			}
			if err := program.buildRecordFrames(context.Background(), nil, program.metadata.Root, reflect.ValueOf(input).Elem().Field(program.metadata.InputField), nil); err != nil {
				t.Fatal(err)
			}
			probe := &preparationFailureProbe{program: program, mode: mode}
			compiled, err := rcompiler.New(rcompiler.Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/preparation-evidence"}}}, InputType: reflect.TypeFor[struct{}]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/preparation-evidence"})
			ctx := context.WithValue(context.Background(), preparationFailureKey{}, probe)
			db := sqlite.New(t)
			_, err = engine.New().Execute(ctx, engine.Request{Input: route, BoundInput: &struct{}{}, DataSource: sqldml.Source{DB: db.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				return nil, program.callAfterValidateInput(ctx, inv.Binder, &fakeCapabilities{})
			})})
			if err == nil || !program.afterValidateInputWasViolated() || program.executionFailure == nil || !strings.Contains(program.executionFailure.Error(), "Original evidence") {
				t.Fatalf("failed callback evidence not retained: err=%v sticky=%v", err, program.executionFailure)
			}
			if strings.HasSuffix(mode, "panic") {
				var panicError *exec.PanicError
				if !errors.As(err, &panicError) {
					t.Fatalf("panic semantics changed: %v", err)
				}
			} else if !errors.Is(err, preparationFailedEvidence) || !strings.Contains(err.Error(), "Original evidence") {
				t.Fatalf("callback error lost violation: %v", err)
			}
			// A violation must never become a native automatic replay request, even if
			// an independently enabled root recovery policy would normally allow one.
			native.transactionRetrySupported, native.recoverySupported, native.scopedSequences = true, true, true
			invocation := rhandler.Invocation{Input: input, Snapshot: program}
			if retry, e := native.RetryTransaction(ctx, invocation, nil, rhandler.MutationOutcome{}); e != nil || retry {
				t.Fatalf("transaction retry=%v error=%v", retry, e)
			}
			if decision, e := native.RecoverMutation(ctx, invocation, nil, rhandler.MutationOutcome{}); e != nil || decision != rhandler.RecoveryNone {
				t.Fatalf("mutation recovery=%v error=%v", decision, e)
			}
			if retry, e := native.RecoverScopedMutation(ctx, invocation, exec.MutationReport{Contention: true}, xhandler.Outcome{}); e != nil || retry {
				t.Fatalf("scoped retry=%v error=%v", retry, e)
			}
		})
	}
}
