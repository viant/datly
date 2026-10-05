package writer_test

import (
	"context"
	"errors"
	"fmt"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/response"
	"reflect"
	"testing"
)

type capturedRecord struct {
	ID int `sqlx:"id,primaryKey=true"`
}
type capturedInput struct {
	Rows        []*capturedRecord `parameter:"Rows,kind=body,in=data" view:"Rows,table=records,auxiliary=true"`
	CurrentRows []*capturedRecord `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=records"`
}
type capturedOutput struct {
	Data   []*capturedRecord `parameter:"Data,kind=output,in=body"`
	Status string
}
type customCapturedBody struct{ cause error }

func (e customCapturedBody) Error() string     { return "custom body" }
func (e customCapturedBody) StatusCode() int   { return 400 }
func (e customCapturedBody) ResponseBody() any { return &capturedOutput{Status: "custom"} }
func (e customCapturedBody) Unwrap() error     { return e.cause }

func capturedWriter(t *testing.T) (*writer.Handler, *capturedInput, rh.Invocation) {
	t.Helper()
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "captured.audit", Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}}
	h, err := writer.New(component, reflect.TypeFor[capturedInput](), reflect.TypeFor[capturedOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	input := &capturedInput{Rows: []*capturedRecord{{ID: 7}}}
	snapshot, err := h.CaptureInput(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	return h, input, rh.Invocation{Input: input, Snapshot: snapshot}
}

func TestCapturedWriterBusinessOutputPreservesCanonicalAndPublicPointers(t *testing.T) {
	h, input, inv := capturedWriter(t)
	payload := &capturedOutput{Data: input.Rows, Status: "business"}
	cause := errors.New("business violation")
	public := &response.Error{Code: 400, Payload: payload, Cause: cause}
	result, err := h.CapturedErrorOutput(t.Context(), inv, fmt.Errorf("initialize: %w", public))
	if err != nil {
		t.Fatal(err)
	}
	canonical, ok := result.(*capturedOutput)
	if !ok || canonical == payload || canonical.Status != "business" || canonical.Data[0] != payload.Data[0] {
		t.Fatalf("canonical=%#v payload=%#v", result, payload)
	}
	canonical.Status = "finalized"
	if public.Payload != payload || payload.Status != "business" || public.Code != 400 || !errors.Is(public, cause) {
		t.Fatalf("public error mutated: %#v", public)
	}
	second, err := h.CapturedErrorOutput(t.Context(), inv, &response.Error{Code: 403, Payload: canonical, Cause: cause})
	if err != nil || second != canonical {
		t.Fatalf("canonical identity changed %T %v", second, err)
	}
}

func TestCapturedWriterDeclinesNoncanonicalAndShadowedBodies(t *testing.T) {
	typed := &response.Error{Code: 400, Payload: &capturedOutput{Status: "inner"}}
	for name, cause := range map[string]error{
		"pure cancellation": context.Canceled,
		"ordinary":          errors.New("plain"),
		"nil":               &response.Error{Code: 400},
		"typed nil":         &response.Error{Code: 400, Payload: (*capturedOutput)(nil)},
		"map":               &response.Error{Code: 400, Payload: map[string]any{"Status": "error"}},
		"value":             &response.Error{Code: 400, Payload: capturedOutput{}},
		"outer nil":         &response.Error{Code: 400, Cause: typed},
		"outer map":         &response.Error{Code: 400, Payload: map[string]any{}, Cause: typed},
		"custom":            customCapturedBody{cause: typed},
		"first joined body": errors.Join(&response.Error{Code: 400}, typed),
	} {
		t.Run(name, func(t *testing.T) {
			h, _, inv := capturedWriter(t)
			result, err := h.CapturedErrorOutput(t.Context(), inv, cause)
			if result != nil || err != nil {
				t.Fatalf("result=%T err=%v", result, err)
			}
		})
	}
}

func TestCapturedWriterRejectsForeignProgramOrInput(t *testing.T) {
	h, _, inv := capturedWriter(t)
	other, _, foreign := capturedWriter(t)
	public := &response.Error{Code: 400, Payload: &capturedOutput{}}
	for name, bad := range map[string]rh.Invocation{
		"missing program": {}, "foreign program": {Input: inv.Input, Snapshot: foreign.Snapshot},
		"different input": {Input: foreign.Input, Snapshot: inv.Snapshot}, "nil input": {Snapshot: inv.Snapshot},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := h.CapturedErrorOutput(context.Background(), bad, public)
			if result != nil || err == nil {
				t.Fatalf("result=%T err=%v", result, err)
			}
		})
	}
	result, err := other.CapturedErrorOutput(t.Context(), inv, public)
	if result != nil || err == nil {
		t.Fatalf("foreign handler accepted %T %v", result, err)
	}
}

func TestCapturedWriterJoinedCancellationPreservesBusinessBody(t *testing.T) {
	h, _, inv := capturedWriter(t)
	payload := &capturedOutput{Status: "business"}
	public := &response.Error{Code: 400, Payload: payload, Cause: errors.New("business cause")}
	cause := errors.Join(context.Canceled, public)
	value, err := h.CapturedErrorOutput(t.Context(), inv, cause)
	if err != nil || value == nil || !errors.Is(cause, context.Canceled) || public.Payload != payload || value.(*capturedOutput).Status != "business" {
		t.Fatalf("result=%T error=%v cause=%v", value, err, cause)
	}
}
