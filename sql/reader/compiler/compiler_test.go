package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestBuildArtifact_CompilesRootSQLTemplateWithParamAliases(t *testing.T) {
	type input struct {
		VendorID int
	}
	component := &spec.Component{
		RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT id FROM vendor WHERE 1=1 #if($vendorID < 0) AND 1=2 #end"}},
		Parameters: []*spec.Parameter{
			{Name: "vendorID", Source: spec.BindSource{Kind: "query", Name: "vendor_id"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	type output struct{ Data []struct{ ID int } }
	artifact, err := BuildArtifact(ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("BuildArtifact failed: %v", err)
	}
	if artifact.Reader.Root.Template == nil {
		t.Fatal("expected root SQL template to be compiled")
	}
}

func TestBuildArtifact_LeavesPlainRootSQLUncompiled(t *testing.T) {
	type input struct {
		ID int
	}
	component := &spec.Component{RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT id FROM vendor WHERE id = :ID"}}}
	type output struct{ Data []struct{ ID int } }
	component.Parameters = []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("BuildArtifact failed: %v", err)
	}
	if artifact.Reader.Root.Template != nil {
		t.Fatal("plain SQL must not carry a compiled template")
	}
}

func TestBuildArtifact_DoesNotCompileNonReaderVeltyBody(t *testing.T) {
	type input struct{ ID int }
	component := &spec.Component{RootView: &spec.View{Source: &spec.ViewSource{SQL: "#if($ID > 0) $sql.Insert($Unsafe, 'users') #end"}}}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeOf(input{})})
	if err != nil {
		t.Fatalf("BuildArtifact failed: %v", err)
	}
	if artifact.Reader.Root.Template != nil {
		t.Fatal("non-reader Velty body must not be compiled as a reader SQL template")
	}
}

func TestBuildArtifact_CompilesNestedViewSQLTemplates(t *testing.T) {
	type input struct{ Include bool }
	type settings struct {
		ID        int
		ProfileID int
	}
	type profile struct {
		ID       int
		UserID   int
		Settings *settings `view:"settings" sql:"#if($include) SELECT id, profile_id FROM settings WHERE 1=1 $View.ParentJoinOn(\"AND\",\"profile_id\") #end" on:"ID:id=ProfileID:profile_id"`
	}
	type row struct {
		ID      int
		Profile *profile `view:"profile" sql:"#if($include) SELECT id, user_id FROM profiles WHERE 1=1 $View.ParentJoinOn(\"AND\",\"user_id\") #end" on:"ID:id=UserID:user_id"`
	}
	type output struct{ Data []*row }
	component := &spec.Component{
		RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT id FROM users"}},
		Parameters: []*spec.Parameter{
			{Name: "include", Source: spec.BindSource{Kind: "query", Name: "include"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("BuildArtifact failed: %v", err)
	}
	if artifact.Reader.Root.Template != nil {
		t.Fatal("plain root view must remain uncompiled")
	}
	if len(artifact.Reader.Root.View.Relations) != 1 || artifact.Reader.Root.View.Relations[0].Of == nil {
		t.Fatal("expected profile relation")
	}
	profilePlan := artifact.Reader.Root.Relations[0].Target
	if profilePlan == nil || profilePlan.Template == nil || len(profilePlan.Relations) != 1 {
		t.Fatal("expected compiled profile and nested settings relation")
	}
	if profilePlan.Relations[0].Target == nil || profilePlan.Relations[0].Target.Template == nil {
		t.Fatal("expected compiled nested settings template")
	}
}

func TestBuildArtifact_CompilesOutputRelationWithNamedParentNonWindowSQL(t *testing.T) {
	type input struct{ Include bool }
	type total struct{ Count int }
	type output struct {
		Data   []struct{ ID int }
		Totals total
	}
	component := &spec.Component{
		Name: "UsersComponent",
		RootView: &spec.View{
			Name:   "Users",
			Source: &spec.ViewSource{SQL: "SELECT id FROM users"},
			Relations: []*spec.Relation{{
				Name: "Totals", Kind: spec.RelationKindDerived, Holder: "Totals", Cardinality: spec.CardinalityOne,
				View: &spec.View{Name: "Totals", Source: &spec.ViewSource{SQL: `#if($include) SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent #end`}},
			}},
		},
		Parameters: []*spec.Parameter{
			{Name: "include", Source: spec.BindSource{Kind: "query", Name: "include"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Totals", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("BuildArtifact failed: %v", err)
	}
	if len(artifact.Reader.Root.View.Relations) != 1 || artifact.Reader.Root.View.Relations[0].Of == nil {
		t.Fatal("expected output relation")
	}
	if artifact.Reader.Root.Relations[0].Target == nil || artifact.Reader.Root.Relations[0].Target.Template == nil {
		t.Fatal("expected named parent non-window output relation program")
	}
}

func TestArtifactBuilder_RejectsNamedParentAccessForSharedView(t *testing.T) {
	shared := &data.View{Spec: spec.View{Name: "Shared",
		Source: &spec.ViewSource{SQL: `#if($include) SELECT * FROM ($View.Left.NonWindowSQL) parent #end`}},
	}
	left := &data.View{Spec: spec.View{Name: "Left"}, Relations: []*data.Relation{{Of: &data.RelationRef{View: shared}}}}
	right := &data.View{Spec: spec.View{Name: "Right"}, Relations: []*data.Relation{{Of: &data.RelationRef{View: shared}}}}
	root := &data.View{Spec: spec.View{Name: "Root"}, Relations: []*data.Relation{
		{Of: &data.RelationRef{View: left}},
		{Of: &data.RelationRef{View: right}},
	}}
	_, err := (&planCompiler{}).sqlTemplateParentAliases(root)
	if err == nil || !strings.Contains(err.Error(), "named parent non-window access") || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected shared-view alias ambiguity, got %v", err)
	}
}

func TestArtifactBuilder_AllowsRelativeParentAccessForSharedView(t *testing.T) {
	shared := &data.View{Spec: spec.View{Name: "Shared",
		Source: &spec.ViewSource{SQL: `#if($include) SELECT * FROM ($View.NonWindowSQL) parent #end`}},
	}
	left := &data.View{Spec: spec.View{Name: "Left"}, Relations: []*data.Relation{{Of: &data.RelationRef{View: shared}}}}
	right := &data.View{Spec: spec.View{Name: "Right"}, Relations: []*data.Relation{{Of: &data.RelationRef{View: shared}}}}
	root := &data.View{Spec: spec.View{Name: "Root"}, Relations: []*data.Relation{
		{Of: &data.RelationRef{View: left}},
		{Of: &data.RelationRef{View: right}},
	}}
	aliases, err := (&planCompiler{}).sqlTemplateParentAliases(root)
	if err != nil {
		t.Fatalf("relative shared-view access failed: %v", err)
	}
	if len(aliases[shared]) != 2 {
		t.Fatalf("expected both parent aliases, got %#v", aliases[shared])
	}
}
