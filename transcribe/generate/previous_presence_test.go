package generate

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestPreviousComparisonReusesConflictingSliceAlias(t *testing.T) {
	for _, alias := range []string{"r", "prev", "present", "has"} {
		t.Run(alias, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "support.go", fmt.Sprintf("package patch\nimport %s \"slices\"\nfunc existing(a,b []int)bool{return %s.Equal(a,b)}", alias, alias), 0)
			if err != nil {
				t.Fatal(err)
			}
			plan := &Plan{Package: "example/patch", Views: []ViewPlan{{Name: "Root", Ownership: ViewGenerated, SetMarkerFields: []string{"Codes"}, Fields: []Field{
				{Name: "Codes", Type: "[]int", Tag: `sqlx:"CODES"`},
				{Name: "Has", Type: "*RootHas", Tag: `setMarker:"true" sqlx:"-"`},
			}}}, EntitySupport: &EntitySupportPlan{File: file}}
			r := &planResolver{input: Input{nativeMutationTargetSelected: true}, plan: plan}
			if err = r.resolvePreviousPresence(); err != nil {
				t.Fatal(err)
			}
			source, err := plan.EntitySupport.source("patch")
			if err != nil {
				t.Fatal(err)
			}
			fset := token.NewFileSet()
			var files []*ast.File
			for name, text := range map[string]string{"support.go": source, "views.go": viewFile("patch", plan)} {
				parsed, err := parser.ParseFile(fset, name, text, parser.AllErrors)
				if err != nil {
					t.Fatal(err)
				}
				files = append(files, parsed)
			}
			config := types.Config{Importer: importer.Default()}
			if _, err = config.Check(plan.Package, fset, files, nil); err != nil {
				t.Fatalf("alias %s: %v\n%s", alias, err, source)
			}
		})
	}
}

func TestNativeMutationGeneratesPreviousComparison(t *testing.T) {
	component, root, child := setMarkerComponent()
	rootID, _ := root.Identity()
	childID, _ := child.Identity()
	plan, err := New(Input{Component: component, SetMarkerViews: map[string]bool{rootID: true, childID: true}, nativeMutationTargetSelected: true}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.EntitySupport == nil {
		t.Fatal("native mutation has no generated Previous comparison")
	}
	source, err := plan.EntitySupport.source(plan.PackageName())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.EntitySupport.Methods) != 2 || strings.Count(source, "SynchronizePresentMarker(") != 2 {
		t.Fatalf("wrong method ownership: %s", source)
	}
	if strings.Contains(source, "PresenceChanged") {
		t.Fatal("row-local comparison recursively processes children")
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for name, text := range map[string]string{"views.go": viewFile(plan.PackageName(), plan), "setters.go": source} {
		file, err := parser.ParseFile(fset, name, text, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	config := types.Config{Importer: importer.Default()}
	if _, err = config.Check(plan.Package, fset, files, nil); err != nil {
		t.Fatalf("generated comparison does not compile: %v\n%s", err, source)
	}
}

func TestPreviousComparisonLeavesLinkedShapesWithOwner(t *testing.T) {
	r := &planResolver{input: Input{nativeMutationTargetSelected: true}, plan: &Plan{Package: "example/patch", Views: []ViewPlan{{Name: "Borrowed", Type: "models.Borrowed", Ownership: ViewLinked, SetMarkerFields: []string{"Name"}}}}}
	if err := r.resolvePreviousPresence(); err != nil {
		t.Fatal(err)
	}
	if r.plan.EntitySupport != nil {
		t.Fatal("generated a method on a borrowed shape")
	}
}

func TestPreviousComparisonDoesNotRestrictAuxiliaryTypes(t *testing.T) {
	view := &spec.View{Name: "Lookup", Auxiliary: true}
	identity, err := view.Identity()
	if err != nil {
		t.Fatal(err)
	}
	r := &planResolver{input: Input{nativeMutationTargetSelected: true, Component: &spec.Component{RootView: view}}, plan: &Plan{Package: "example/patch", Views: []ViewPlan{{Identity: identity, Name: "Lookup", Ownership: ViewGenerated, SetMarkerFields: []string{"Data"}, Fields: []Field{{Name: "Data", Type: "map[string]any"}, {Name: "Has", Type: "*LookupHas", Tag: `setMarker:"true"`}}}}}}
	if err = r.resolvePreviousPresence(); err != nil {
		t.Fatal(err)
	}
	if r.plan.EntitySupport != nil {
		t.Fatal("nonmutating auxiliary type acquired comparison restrictions")
	}
}

func TestPreviousComparisonRelocatesWithSliceShape(t *testing.T) {
	const owner = "example/models"
	shape := ViewPlan{Name: "Root", Type: "Root", Ownership: ViewGenerated, SetMarkerFields: []string{"Codes"}, Fields: []Field{
		{Name: "Codes", Type: "[]int", Tag: `sqlx:"CODES"`},
		{Name: "Has", Type: "*RootHas", Tag: `setMarker:"true" sqlx:"-"`},
	}}
	plan := &Plan{Package: "example/patch", Views: []ViewPlan{shape}}
	r := &planResolver{input: Input{nativeMutationTargetSelected: true}, plan: plan}
	if err := r.resolvePreviousPresence(); err != nil {
		t.Fatal(err)
	}
	target := &Plan{Package: owner, Views: []ViewPlan{shape}}
	destinations := &shapeDestinations{byName: map[string]shapeDestination{
		"Root": {name: "Root", pkg: owner, file: "row.go"}, "RootHas": {name: "RootHas", pkg: owner, file: "row.go"},
	}}
	if err := destinations.relocateEntityMethods(plan, map[string]*Plan{owner: target}); err != nil {
		t.Fatal(err)
	}
	if len(plan.EntitySupport.Methods) != 0 || target.EntitySupport == nil || len(target.EntitySupport.Methods) != 1 {
		t.Fatal("comparison did not move with its owner")
	}
	source, err := target.EntitySupport.source(target.PackageName())
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for name, text := range map[string]string{"methods.go": source, "row.go": viewFile(target.PackageName(), target)} {
		file, err := parser.ParseFile(fset, name, text, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	config := types.Config{Importer: importer.Default()}
	if _, err = config.Check(owner, fset, files, nil); err != nil {
		t.Fatalf("relocated comparison does not compile: %v\n%s", err, source)
	}
}
