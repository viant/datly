package readerbuilder

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
)

type builderArgumentHandler struct{}

func (*builderArgumentHandler) Compute(context.Context, any, ...any) (*xpredicate.Criteria, error) {
	return nil, nil
}

func argumentService(t *testing.T) *Service {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[builderArgumentHandler](), x.WithPkgPath("example.com/security"), x.WithName("Scope"))); err != nil {
		t.Fatal(err)
	}
	return New(Config{Name: "Records", Scope: "example.com/records", Types: catalog})
}

func TestPredicateMutationTypedArgumentsJSONToInspection(t *testing.T) {
	service := argumentService(t)
	raw := fmt.Sprintf(`{"dql":%q,"operation":{"type":"addFieldPredicate","predicate":{"field":"IDs","view":"records","group":0,"name":"handler","args":["example.com/security.Scope"],"handlerArgs":["limit",10,"enabled",false,"nullable",null,"quoted","a\"b,c"]}}}`, baseDQL)
	var request Request
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	expected := []any{"limit", 10, "enabled", false, "nullable", nil, "quoted", "a\"b,c"}
	if !reflect.DeepEqual(*request.Operation.Predicate.HandlerArgs, expected) {
		t.Fatalf("decoded types: %#v", request.Operation.Predicate.HandlerArgs)
	}
	cloned := request.Operation.Predicate.predicate()
	cloned.HandlerArgs[0] = "changed"
	if (*request.Operation.Predicate.HandlerArgs)[0] != "limit" {
		t.Fatal("mutation shares configuration with rendered predicate")
	}
	response := service.Apply(context.Background(), request)
	if !response.Applied || len(response.Diagnostics) != 0 {
		t.Fatalf("response=%+v", response)
	}
	if got := response.Structure.Component.Parameters[0].Predicates[0].HandlerArgs; !reflect.DeepEqual(got, expected) {
		t.Fatalf("inspection lost args: %#v", got)
	}
	declarations, err := dql.DeclarationOccurrences(response.DQL)
	if err != nil {
		t.Fatal(err)
	}
	if got := declarations[0].Predicates[0].Predicate.HandlerArgs; !reflect.DeepEqual(got, expected) {
		t.Fatalf("DQL lost args: %#v", got)
	}
	inspected := service.Apply(context.Background(), Request{DQL: response.DQL, Operation: Operation{Type: OperationInspect}})
	if !inspected.Applied || len(inspected.Diagnostics) != 0 || !reflect.DeepEqual(inspected.Structure.Component.Parameters[0].Predicates[0].HandlerArgs, expected) {
		t.Fatalf("inspect=%+v", inspected)
	}

	// Exercise the actual JSON HTTP boundary, including its default decoder.
	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, httptest.NewRequest("POST", "/", strings.NewReader(raw)))
	var httpResponse Response
	if recorder.Code != 200 {
		t.Fatalf("HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &httpResponse); err != nil {
		t.Fatal(err)
	}
	if !httpResponse.Applied || !reflect.DeepEqual(httpResponse.Structure.Component.Parameters[0].Predicates[0].HandlerArgs, expected) {
		t.Fatalf("HTTP response=%s", recorder.Body.String())
	}
}

func TestPredicateMutationOmittedAndExplicitEmptyArguments(t *testing.T) {
	service := argumentService(t)
	source := strings.Replace(baseDQL, ".Optional()", `.Optional().WithPredicate(0,'handler','example.com/security.Scope','limit',10,'enabled',false)`, 1)
	for _, tc := range []struct {
		name, field, predicate string
		args                   []string
		expected               []any
	}{
		{"omitted preserves", "", "handler", []string{"example.com/security.Scope"}, []any{"limit", 10, "enabled", false}},
		{"empty clears", `,"handlerArgs":[]`, "handler", []string{"example.com/security.Scope"}, nil},
		{"switch to builtin", "", "in", []string{"r", "id"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, _ := json.Marshal(tc.args)
			var mutation PredicateMutation
			raw := fmt.Sprintf(`{"field":"IDs","view":"records","group":0,"name":%q,"args":%s%s}`, tc.predicate, args, tc.field)
			if err := json.Unmarshal([]byte(raw), &mutation); err != nil {
				t.Fatal(err)
			}
			// Explicit [] must survive forwarding through a Go JSON client.
			forwarded, err := json.Marshal(mutation)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(forwarded, &mutation); err != nil {
				t.Fatal(err)
			}
			result := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationUpdateFieldPredicate, Predicate: &mutation}})
			if !result.Applied || len(result.Diagnostics) > 0 {
				t.Fatalf("response=%+v", result)
			}
			if got := result.Structure.Component.Parameters[0].Predicates[0].HandlerArgs; !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("got %#v want %#v", got, tc.expected)
			}
		})
	}
	old := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{Field: "IDs", View: "records", Name: "handler", Args: []string{"example.com/security.Scope"}}}})
	if !old.Applied || !strings.Contains(old.DQL, `.WithPredicate(0, "handler", "example.com/security.Scope")`) {
		t.Fatalf("old add=%+v", old)
	}
}

func TestPredicateMutationRejectsInvalidArgumentJSON(t *testing.T) {
	for _, raw := range []string{
		`{"handlerArgs":[1.2]}`, `{"handlerArgs":[{}]}`, `{"handlerArgs":[[]]}`,
		`{"handlerArgs":[999999999999999999999999999999]}`, `{"handlerArgs":[10]} {}`,
		`{"handlerArgs":[10],"unknown":true}`,
	} {
		var mutation PredicateMutation
		if err := mutation.UnmarshalJSON([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestPredicateMutationDoesNotConvertHandlerArgsToBuiltinStrings(t *testing.T) {
	args := []any{10, false}
	response := argumentService(t).Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{Type: OperationAddFieldPredicate, Predicate: &PredicateMutation{Field: "IDs", View: "records", Name: "in", Args: []string{"r", "id"}, HandlerArgs: &args}}})
	if response.Applied || response.DQL != baseDQL || len(response.Diagnostics) == 0 {
		t.Fatalf("accepted typed builtin configuration: %+v", response)
	}
}
