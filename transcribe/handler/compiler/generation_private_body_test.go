package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestBuildInputPreservesPrivateValidationBody(t *testing.T) {
	for _, privateFirst := range []bool{false, true} {
		body := &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "[]*Record"}
		private := &spec.Parameter{Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: `internal:"true" json:"-" mcp:"-"`}
		params := []*spec.Parameter{body, private}
		if privateFirst {
			params[0], params[1] = params[1], params[0]
		}
		component := &spec.Component{Name: "Records", Parameters: params, RootView: &spec.View{Name: "Records", Auxiliary: true, EntityHooks: "BatchLifecycle", Source: &spec.ViewSource{Table: "RECORDS"}}}
		got, err := (&Compiler{}).BuildInput(Request{Component: component, Input: "Rows", Operation: plan.OperationPatch}, "Record")
		if err != nil {
			t.Fatalf("privateFirst=%v: %v", privateFirst, err)
		}
		if got.Input != "Rows" {
			t.Fatalf("private validation body owns mutation: %s", got.Input)
		}
		var retained *spec.Parameter
		for _, p := range got.Component.Parameters {
			if p.Name == "Submitted" {
				retained = p
			}
		}
		if !reflect.DeepEqual(retained, private.Clone()) {
			t.Fatalf("private binding changed: %+v", retained)
		}
	}
}

func TestBuildInputPrivateBodyDoesNotBecomeMutationRoot(t *testing.T) {
	private := &spec.Parameter{Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: `internal:"true"`}
	component := &spec.Component{Name: "Records", Parameters: []*spec.Parameter{private}, RootView: &spec.View{Name: "Records", Auxiliary: true, EntityHooks: "BatchLifecycle", Source: &spec.ViewSource{Table: "RECORDS"}}}
	request := Request{Component: component, Operation: plan.OperationPatch}
	got, err := (&Compiler{}).BuildInput(request, "Record")
	if err != nil {
		t.Fatal(err)
	}
	if got.Input != "Records" || len(got.Component.Parameters) != 2 {
		t.Fatalf("expected separate synthesized mutation root, got %+v", got)
	}
	if !reflect.DeepEqual(got.Component.Parameters[0], private.Clone()) || got.Component.Parameters[1].TypeExpr != "[]*Record" || got.Component.Parameters[1].Required == nil || !*got.Component.Parameters[1].Required {
		t.Fatal("private validation body was adopted or modified")
	}
	request.Input = "Submitted"
	if _, err := (&Compiler{}).BuildInput(request, "Record"); err == nil {
		t.Fatal("explicit private validation input admitted as mutation owner")
	}
	request.Input = ""
	component.Parameters[0] = private.Clone()
	component.Parameters[0].Name = "Records"
	if _, err := (&Compiler{}).BuildInput(request, "Record"); err == nil {
		t.Fatal("synthesized public input collided with private binding")
	}
}

func TestBuildInputPrivateTagDoesNotRelaxPublicAmbiguity(t *testing.T) {
	for _, tag := range []string{"", `internal:"false"`, `internal:"True"`, `json:"-"`, `mcp:"-"`} {
		component := &spec.Component{Name: "Records", RootView: &spec.View{Name: "Records", Auxiliary: true, EntityHooks: "BatchLifecycle"}, Parameters: []*spec.Parameter{
			{Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "[]*Record"},
			{Name: "Other", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: tag},
		}}
		if _, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch}, "Record"); err == nil {
			t.Fatalf("competing public body admitted with %q", tag)
		}
	}
}

func TestBuildInputKeepsTaggedInternalWriterRoot(t *testing.T) {
	component := &spec.Component{Name: "Records", Routes: []*spec.Route{{Method: "PATCH", Path: "/records"}}, RootView: &spec.View{Name: "Records", Auxiliary: true, EntityHooks: "BatchLifecycle"}, Parameters: []*spec.Parameter{
		{Name: "Rows", Source: spec.BindSource{Kind: "internal"}, TypeExpr: "[]*Record", Tag: `internal:"true"`},
	}}
	got, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch}, "Record")
	if err != nil || got.Input != "Rows" {
		t.Fatalf("internal writer root lost: input=%s, err=%v", got.Input, err)
	}
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: `internal:"true"`})
	if _, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch}, "Record"); err == nil {
		t.Fatal("internal writer/body separation guard relaxed")
	}
}
