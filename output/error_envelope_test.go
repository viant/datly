package output

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type errorEnvelopeMeta struct {
	PageCnt *int `json:"pageCnt"`
	Cnt     int  `json:"cnt"`
}
type errorEnvelopeValue struct {
	legacy.Status
	Meta errorEnvelopeMeta `json:"meta"`
	Data []int             `json:"data"`
}
type errorEnvelopePointer struct {
	State *legacy.Status
	Meta  errorEnvelopeMeta
	Data  []int
}

type transportErrorEnvelope struct {
	legacy.Status
	*modern.Buffered
}

type errorEnvelopeNamedPointer *errorEnvelopeValue

var envelopeCallbacks atomic.Int64

func (*errorEnvelopeValue) Init()     { envelopeCallbacks.Add(1) }
func (*errorEnvelopeValue) Finalize() { envelopeCallbacks.Add(1) }
func declaredErrorStatus(name string) *spec.Parameter {
	return &spec.Parameter{Name: name, Source: spec.BindSource{Kind: "output", Name: "status"}}
}

func TestLegacyErrorEnvelopeCompiledOwnerFreshnessAndDirection(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		typ         reflect.Type
		emit        bool
	}{
		{"anonymous-value", "Status", reflect.TypeFor[errorEnvelopeValue](), false},
		{"pointer-root", "Status", reflect.TypeFor[*errorEnvelopeValue](), false},
		{"named-pointer-root", "Status", reflect.TypeFor[errorEnvelopeNamedPointer](), false},
		{"named-pointer-holder", "State", reflect.TypeFor[errorEnvelopePointer](), false},
		{"original-output-direction", "Status", reflect.TypeFor[errorEnvelopeValue](), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parameter := declaredErrorStatus(tc.field)
			if tc.emit {
				parameter.Source.Kind = "body"
				parameter.EmitOutput = true
			}
			component := &spec.Component{Parameters: []*spec.Parameter{parameter}}
			envelopeCallbacks.Store(0)
			p, e := (Compiler{}).Compile(CompileInput{Component: component, Type: tc.typ})
			if e != nil {
				t.Fatal(e)
			}
			// Requests must retain compiled owner authority, independent of mutable source metadata.
			parameter.Name = "Unrelated"
			component.Parameters = nil
			var holders []*legacy.Status
			for _, message := range []string{"first", "second"} {
				value, applies, e := p.ErrorEnvelope(message)
				if e != nil || !applies || reflect.TypeOf(value) != tc.typ {
					t.Fatalf("declared root %T applicable%v error%v", value, applies, e)
				}
				rv := reflect.ValueOf(value)
				if rv.Kind() == reflect.Pointer {
					rv = rv.Elem()
				}
				status := rv.FieldByName(tc.field)
				if status.Kind() == reflect.Pointer {
					holders = append(holders, status.Interface().(*legacy.Status))
					status = status.Elem()
				}
				got := status.Interface().(legacy.Status)
				if got.Status != "error" || got.Message != message || got.Errors != nil || got.Warning != nil || got.Extras != nil || !rv.FieldByName("Meta").IsZero() || !rv.FieldByName("Data").IsNil() {
					t.Fatalf("fresh zero root changed: %+v", got)
				}
			}
			if len(holders) == 2 && holders[0] == holders[1] {
				t.Fatal("status instance retained across requests")
			}
			if envelopeCallbacks.Load() != 0 {
				t.Fatal("envelope factory invoked callbacks")
			}
		})
	}
}

func TestLegacyErrorEnvelopeConcurrentPlanImmutability(t *testing.T) {
	p, e := (Compiler{}).Compile(CompileInput{Component: &spec.Component{Parameters: []*spec.Parameter{declaredErrorStatus("State")}}, Type: reflect.TypeFor[*errorEnvelopePointer]()})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	seen := sync.Map{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok, e := p.ErrorEnvelope("public")
			if e != nil || !ok {
				t.Errorf("factory %v %v", ok, e)
				return
			}
			out := v.(*errorEnvelopePointer)
			if _, shared := seen.LoadOrStore(out.State, true); shared {
				t.Error("shared holder")
			}
			out.State.Extras = map[string]interface{}{"caller": 1}
			out.Data = []int{1}
		}()
	}
	wg.Wait()
	v, _, e := p.ErrorEnvelope("after")
	if e != nil {
		t.Fatal(e)
	}
	out := v.(*errorEnvelopePointer)
	if out.State.Extras != nil || out.Data != nil || out.State.Message != "after" {
		t.Fatal("request mutation escaped into compiled plan")
	}
}

func TestLegacyErrorEnvelopeExactAuthorityRejections(t *testing.T) {
	type defined legacy.Status
	type namedString string
	type lookalike struct {
		Status  string
		Message string
		Errors  interface{}
		Warning []*legacy.Warning
		Extras  map[string]interface{}
	}
	for _, tc := range []struct {
		name       string
		typ        reflect.Type
		parameters []*spec.Parameter
		reject     bool
	}{
		{"modern", reflect.TypeFor[struct{ Status modern.Status }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"scalar", reflect.TypeFor[struct{ Status string }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"named-string", reflect.TypeFor[struct{ Status namedString }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"defined-lookalike", reflect.TypeFor[struct{ Status defined }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"structural-lookalike", reflect.TypeFor[struct{ Status lookalike }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"ordinary-field", reflect.TypeFor[errorEnvelopeValue](), nil, false},
		{"explicit-transport-output", reflect.TypeFor[transportErrorEnvelope](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"input-status", reflect.TypeFor[errorEnvelopeValue](), []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "body", Name: "status"}}}, false},
		{"absent-field", reflect.TypeFor[struct{ Data []int }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"no-status", reflect.TypeFor[struct{ Data []int }](), nil, false},
		{"unsupported-root", reflect.TypeFor[**errorEnvelopeValue](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"unsupported-holder-depth", reflect.TypeFor[struct{ Status **legacy.Status }](), []*spec.Parameter{declaredErrorStatus("Status")}, false},
		{"multiple-legacy", reflect.TypeFor[struct {
			First  legacy.Status
			Second *legacy.Status
		}](), []*spec.Parameter{declaredErrorStatus("First"), declaredErrorStatus("Second")}, true},
		{"mixed-multiple", reflect.TypeFor[struct {
			First  legacy.Status
			Second modern.Status
		}](), []*spec.Parameter{declaredErrorStatus("First"), declaredErrorStatus("Second")}, true},
		{"multiple-modern", reflect.TypeFor[struct {
			First  modern.Status
			Second string
		}](), []*spec.Parameter{declaredErrorStatus("First"), declaredErrorStatus("Second")}, false},
		{"unwritable-legacy", reflect.TypeFor[struct{ state *legacy.Status }](), []*spec.Parameter{declaredErrorStatus("state")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, e := (Compiler{}).Compile(CompileInput{Type: tc.typ, Component: &spec.Component{Parameters: tc.parameters}})
			if tc.reject {
				if e == nil {
					t.Fatal("malformed eligible legacy authority accepted")
				}
				return
			}
			if e != nil {
				t.Fatal("unrelated output compilation changed", e)
			}
			v, ok, e := p.ErrorEnvelope("public")
			if e != nil || ok || v != nil {
				t.Fatal("nonlegacy authority admitted", v, ok, e)
			}
		})
	}
}
