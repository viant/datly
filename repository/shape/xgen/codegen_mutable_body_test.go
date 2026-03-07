package xgen

import (
	"reflect"
	"strings"
	"testing"

	shapeload "github.com/viant/datly/repository/shape/load"
	shapeplan "github.com/viant/datly/repository/shape/plan"
	shapeast "github.com/viant/datly/repository/shape/velty/ast"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type mutableBodyFoos struct {
	Id              *int
	Name            *string
	FoosPerformance []*mutableBodyFoosPerformance `view:",table=FOOS_PERFORMANCE" on:"Id:ID=FooId:FOO_ID"`
}

type mutableBodyFoosPerformance struct {
	Id    *int
	FooId *int
	Name  *string
}

func TestComponentCodegen_BuildMutableVeltyBlock_PatchOne(t *testing.T) {
	codegen, inputType, support := newMutableBodyFixture("PATCH", false)
	actual := renderMutableBlock(t, codegen, inputType, support)
	for _, fragment := range []string{
		`$sequencer.Allocate("FOOS", $Foos, "Id")`,
		`#set($CurFoosById = $CurFoos.IndexBy("Id"))`,
		`#if($Foos)`,
		`#if($CurFoosById.HasKey($Foos.Id) == true)`,
		`$sql.Update($Foos, "FOOS");`,
		`$sql.Insert($Foos, "FOOS");`,
	} {
		if !strings.Contains(actual, fragment) {
			t.Fatalf("expected fragment %q in generated body:\n%s", fragment, actual)
		}
	}
}

func TestComponentCodegen_BuildMutableVeltyBlock_PatchMany(t *testing.T) {
	codegen, inputType, support := newMutableBodyFixture("PATCH", true)
	actual := renderMutableBlock(t, codegen, inputType, support)
	for _, fragment := range []string{
		`$sequencer.Allocate("FOOS", $Foos, "Id")`,
		`#set($CurFoosById = $CurFoos.IndexBy("Id"))`,
		`#foreach($RecFoos in $Foos)`,
		`#if($CurFoosById.HasKey($RecFoos.Id) == true)`,
		`$sql.Update($RecFoos, "FOOS");`,
		`$sql.Insert($RecFoos, "FOOS");`,
	} {
		if !strings.Contains(actual, fragment) {
			t.Fatalf("expected fragment %q in generated body:\n%s", fragment, actual)
		}
	}
}

func TestComponentCodegen_BuildMutableVeltyBlock_PutOne(t *testing.T) {
	codegen, inputType, support := newMutableBodyFixture("PUT", false)
	actual := renderMutableBlock(t, codegen, inputType, support)
	if strings.Contains(actual, `$sequencer.Allocate(`) {
		t.Fatalf("did not expect sequence allocation in PUT body:\n%s", actual)
	}
	if strings.Contains(actual, `$sql.Insert($Foos, "FOOS");`) {
		t.Fatalf("did not expect insert branch in PUT body:\n%s", actual)
	}
	for _, fragment := range []string{
		`#set($CurFoosById = $CurFoos.IndexBy("Id"))`,
		`#if($Foos)`,
		`#if($CurFoosById.HasKey($Foos.Id) == true)`,
		`$sql.Update($Foos, "FOOS");`,
	} {
		if !strings.Contains(actual, fragment) {
			t.Fatalf("expected fragment %q in generated body:\n%s", fragment, actual)
		}
	}
}

func TestComponentCodegen_BuildMutableVeltyBlock_PostMany(t *testing.T) {
	codegen, inputType, support := newMutableBodyFixture("POST", true)
	actual := renderMutableBlock(t, codegen, inputType, support)
	if strings.Contains(actual, `HasKey`) || strings.Contains(actual, `$sql.Update(`) {
		t.Fatalf("did not expect update logic in POST body:\n%s", actual)
	}
	for _, fragment := range []string{
		`$sequencer.Allocate("FOOS", $Foos, "Id")`,
		`#set($CurFoosById = $CurFoos.IndexBy("Id"))`,
		`#foreach($RecFoos in $Foos)`,
		`$sql.Insert($RecFoos, "FOOS");`,
	} {
		if !strings.Contains(actual, fragment) {
			t.Fatalf("expected fragment %q in generated body:\n%s", fragment, actual)
		}
	}
}

func TestComponentCodegen_BuildMutableVeltyBlock_PatchManyMany(t *testing.T) {
	codegen, inputType, support := newMutableBodyFixture("PATCH", true)
	actual := renderMutableBlock(t, codegen, inputType, support)
	for _, fragment := range []string{
		`$sequencer.Allocate("FOOS_PERFORMANCE", $Foos, "FoosPerformance/Id")`,
		`#foreach($RecFoosPerformance in $RecFoos.FoosPerformance)`,
		`#set($RecFoosPerformance.FooId = $RecFoos.Id)`,
		`#if($CurFoosPerformanceById.HasKey($RecFoosPerformance.Id) == true)`,
		`$sql.Update($RecFoosPerformance, "FOOS_PERFORMANCE");`,
		`$sql.Insert($RecFoosPerformance, "FOOS_PERFORMANCE");`,
	} {
		if !strings.Contains(actual, fragment) {
			t.Fatalf("expected fragment %q in generated body:\n%s", fragment, actual)
		}
	}
}

func TestComponentCodegen_BuildMutableVeltyBlock_PutOneMany(t *testing.T) {
	codegen, inputType, support := newMutableBodyFixture("PUT", false)
	actual := renderMutableBlock(t, codegen, inputType, support)
	if strings.Contains(actual, `$sql.Insert($RecFoosPerformance, "FOOS_PERFORMANCE");`) {
		t.Fatalf("did not expect child insert branch in PUT body:\n%s", actual)
	}
	for _, fragment := range []string{
		`#foreach($RecFoosPerformance in $Foos.FoosPerformance)`,
		`#set($RecFoosPerformance.FooId = $Foos.Id)`,
		`#if($CurFoosPerformanceById.HasKey($RecFoosPerformance.Id) == true)`,
		`$sql.Update($RecFoosPerformance, "FOOS_PERFORMANCE");`,
	} {
		if !strings.Contains(actual, fragment) {
			t.Fatalf("expected fragment %q in generated body:\n%s", fragment, actual)
		}
	}
}

func newMutableBodyFixture(method string, many bool) (*ComponentCodegen, reflect.Type, *mutableComponentSupport) {
	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{Name: "CurFoos", Template: &view.Template{Source: "SELECT * FROM FOOS WHERE ID IN (?)"}},
		&view.View{Name: "CurFoosPerformance", Template: &view.Template{Source: "SELECT * FROM FOOS_PERFORMANCE WHERE ID IN (?)"}},
	)
	codegen := &ComponentCodegen{Component: &shapeload.Component{
		Method: method,
		Input: []*shapeplan.State{
			{Parameter: state.Parameter{Name: "CurFoos", In: state.NewViewLocation("CurFoos"), Tag: `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`}},
			{Parameter: state.Parameter{Name: "CurFoosPerformance", In: state.NewViewLocation("CurFoosPerformance"), Tag: `view:"CurFoosPerformance" sql:"uri=foos/cur_foos_performance.sql"`}},
		},
	}, Resource: resource}
	bodyType := reflect.TypeOf(&mutableBodyFoos{})
	if many {
		bodyType = reflect.TypeOf([]*mutableBodyFoos{})
	}
	inputType := reflect.StructOf([]reflect.StructField{
		{Name: "Foos", Type: bodyType},
		{Name: "CurFoos", Type: reflect.TypeOf([]*mutableBodyFoos{})},
	})
	support := &mutableComponentSupport{
		BodyFieldName: "Foos",
		Helpers: []mutableIndexHelper{
			{
				ViewParamName: "CurFoos",
				ViewFieldName: "CurFoos",
				MapFieldName:  "CurFoosById",
				KeyFieldName:  "Id",
				ItemTypeExpr:  "*xgen.mutableBodyFoos",
			},
			{
				ViewParamName: "CurFoosPerformance",
				ViewFieldName: "CurFoosPerformance",
				MapFieldName:  "CurFoosPerformanceById",
				KeyFieldName:  "Id",
				ItemTypeExpr:  "*xgen.mutableBodyFoosPerformance",
			},
		},
	}
	return codegen, inputType, support
}

func renderMutableBlock(t *testing.T, codegen *ComponentCodegen, inputType reflect.Type, support *mutableComponentSupport) string {
	t.Helper()
	block, err := codegen.buildMutableVeltyBlock(inputType, support)
	if err != nil {
		t.Fatalf("build mutable body: %v", err)
	}
	builder := shapeast.NewBuilder(shapeast.Options{Lang: shapeast.LangVelty})
	if err = block.Generate(builder); err != nil {
		t.Fatalf("generate mutable body: %v", err)
	}
	return strings.TrimSpace(builder.String())
}
