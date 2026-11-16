package selectorcompanion

import (
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type preparationFixture struct {
	input     reflect.Type
	route     spec.RouteRef
	plan      *bindly.Plan
	selectors *BindingPlan
}

func (f *preparationFixture) Type() reflect.Type         { return f.input }
func (f *preparationFixture) Route() spec.RouteRef       { return f.route }
func (f *preparationFixture) Plan() *bindly.Plan         { return f.plan }
func (f *preparationFixture) SelectorPlan() *BindingPlan { return f.selectors }
func TestPreparationBindingExactIdentity(t *testing.T) {
	contract := &preparationFixture{input: reflect.TypeFor[struct{ ID int }](), plan: new(bindly.Plan), route: spec.RouteRef{Method: "GET", Path: "/records"}}
	handler, reader := new(int), new(int)
	proof := NewPreparationBinding(handler, reader, contract)
	if !proof.Matches(handler, contract) {
		t.Fatal("exact binding rejected")
	}
	copyProof := *proof
	detached := *contract
	for name, ok := range map[string]bool{"nil": (*PreparationBinding)(nil).Matches(handler, contract), "zero": new(PreparationBinding).Matches(handler, contract), "copy": copyProof.Matches(handler, contract), "handler": proof.Matches(new(int), contract), "contract": proof.Matches(handler, &detached)} {
		if ok {
			t.Fatalf("%s admitted", name)
		}
	}
	oldPlan := contract.plan
	contract.plan = new(bindly.Plan)
	if proof.Matches(handler, contract) {
		t.Fatal("plan substitution admitted")
	}
	contract.plan = oldPlan
	contract.selectors = new(BindingPlan)
	if proof.Matches(handler, contract) {
		t.Fatal("selector substitution admitted")
	}
}
func TestPreparationBindingOnlyDispatcherMints(t *testing.T) {
	root := filepath.Join("..", "..")
	calls := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == ".cache" || d.Name() == "graphify-out" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(source, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			selected, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selected.Sel.Name == "NewPreparationBinding" {
				calls++
				if filepath.Clean(path) != filepath.Join(root, "runtime", "component.go") {
					t.Errorf("unexpected mint: %s", path)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("mint call count %d", calls)
	}
}
