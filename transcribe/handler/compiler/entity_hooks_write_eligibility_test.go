package compiler

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	model "github.com/viant/x/syntetic/model"
	h "github.com/viant/xdatly/handler"
)

type eligibleEntityHooks struct{ rootEntityHooks }

func (*eligibleEntityHooks) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], h.WriteAction) (bool, error) {
	return true, nil
}

type promotedEligibleEntityHooks struct{ eligibleEntityHooks }
type eligibilityWrongReturn struct{ rootEntityHooks }

func (*eligibilityWrongReturn) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], h.WriteAction) error {
	return nil
}

type eligibilityWrongBool struct{ rootEntityHooks }

func (*eligibilityWrongBool) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], h.WriteAction) (int, error) {
	return 0, nil
}

type eligibilityWrongAction struct{ rootEntityHooks }

func (*eligibilityWrongAction) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], string) (bool, error) {
	return true, nil
}

type eligibilityWrongEntity struct{ rootEntityHooks }

func (*eligibilityWrongEntity) WriteEligible(context.Context, *hookParent, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], h.WriteAction) (bool, error) {
	return true, nil
}

type eligibilityWrongOutput struct{ rootEntityHooks }

func (*eligibilityWrongOutput) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookInput], h.WriteAction) (bool, error) {
	return true, nil
}

type eligibilityWrongParent struct{ rootEntityHooks }

func (*eligibilityWrongParent) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, hookParent, hookOutput], h.WriteAction) (bool, error) {
	return true, nil
}

type eligibilityWrongContext struct{ rootEntityHooks }

func (*eligibilityWrongContext) WriteEligible(string, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], h.WriteAction) (bool, error) {
	return true, nil
}

type eligibilityVariadicHooks struct{ rootEntityHooks }

func (*eligibilityVariadicHooks) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput], ...h.WriteAction) (bool, error) {
	return true, nil
}

type eligibilityChildHooks struct{ childEntityHooks }

func (*eligibilityChildHooks) WriteEligible(context.Context, *hookEntity, h.LifecycleContext[hookEntity, hookParent, hookOutput], h.WriteAction) (bool, error) {
	return true, nil
}

func TestEntityHookCompilerWriteEligibilityContracts(t *testing.T) {
	for _, tc := range []struct {
		value              any
		invalid, component bool
		parent             string
	}{
		{value: rootEntityHooks{}}, {value: eligibleEntityHooks{}}, {value: promotedEligibleEntityHooks{}},
		{value: eligibilityWrongReturn{}, invalid: true}, {value: eligibilityWrongBool{}, invalid: true}, {value: eligibilityWrongAction{}, invalid: true}, {value: eligibilityWrongEntity{}, invalid: true}, {value: eligibilityWrongOutput{}, invalid: true}, {value: eligibilityWrongParent{}, invalid: true}, {value: eligibilityWrongContext{}, invalid: true}, {value: eligibilityVariadicHooks{}, invalid: true},
		{value: eligibleEntityHooks{}, invalid: true, component: true},
		{value: eligibilityChildHooks{}, invalid: true, parent: reflect.TypeFor[hookParent]().PkgPath() + ".hookParent"},
	} {
		typ := reflect.TypeOf(tc.value)
		name := typ.Name()
		if tc.component {
			name += "/auxiliary"
		}
		t.Run(name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: typ.PkgPath()})
			if err != nil {
				t.Fatal(err)
			}
			_, err = (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: typ.Name(), Entity: typ.PkgPath() + ".hookEntity", Input: typ.PkgPath() + ".hookInput", Output: typ.PkgPath() + ".hookOutput", Parent: tc.parent, Component: tc.component, WriteEligibilityAllowed: true})
			if (err != nil) != tc.invalid {
				t.Fatalf("Compile error=%v invalid=%v", err, tc.invalid)
			}
			if tc.invalid && !strings.Contains(err.Error(), "WriteEligible") {
				t.Fatalf("failure did not identify optional eligibility method: %v", err)
			}
		})
	}
}

func TestEntityHookCompilerWriteEligibilityGenericPromotedAndCanonicalContracts(t *testing.T) {
	location := reflect.TypeFor[hookEntity]().PkgPath()
	source := `package hooks
 type Hook[T,P any] struct{}
 func(*Hook[A,B])Init(c.Context,*A,h.LifecycleContext[A,B,e.hookOutput])error{return nil}
 func(*Hook[T,P])Validate(c.Context,*T,h.LifecycleContext[T,P,e.hookOutput])error{return nil}
 func(*Hook[T,P])WriteEligible(c.Context,*T,h.LifecycleContext[T,P,e.hookOutput],h.WriteAction)(bool,error){return true,nil}
 type Wrapper struct{Hook[e.hookEntity,h.NoParent]}
 type WrongAction[T,P any]struct{Hook[T,P]}
 func(*WrongAction[T,P])WriteEligible(c.Context,*T,h.LifecycleContext[T,P,e.hookOutput],string)(bool,error){return true,nil}
 type Imposter struct{Hook[e.hookEntity,h.NoParent]}
 func(*Imposter)WriteEligible(c.Context,*e.hookEntity,other.LifecycleContext[e.hookEntity,h.NoParent,e.hookOutput],h.WriteAction)(bool,error){return true,nil}
 `
	file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]*x.Type{}
	for _, decl := range file.Decls {
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.TYPE {
			for _, item := range group.Specs {
				def := item.(*ast.TypeSpec)
				types[def.Name.Name] = &x.Type{Name: def.Name.Name, PkgPath: "example.com/eligibility", SynteticType: &model.Type{Name: def.Name.Name, PkgPath: "example.com/eligibility", TypeSpec: def, Imports: map[string]*model.ImportRef{"c": {Path: "context"}, "h": {Path: "github.com/viant/xdatly/handler"}, "e": {Path: location}, "other": {Path: "example.com/other"}}}}
			}
		}
	}
	for _, decl := range file.Decls {
		if method, ok := decl.(*ast.FuncDecl); ok {
			receiver, err := (xshape.Resolver{}).Canonical(method.Recv.List[0].Type)
			if err != nil {
				t.Fatal(err)
			}
			reference, err := (xshape.Resolver{}).Reference(receiver)
			if err != nil {
				t.Fatal(err)
			}
			types[reference.BaseName].SynteticType.PtrMethodsAST = append(types[reference.BaseName].SynteticType.PtrMethodsAST, method)
		}
	}
	catalog := typecatalog.NewCatalog()
	for _, typ := range types {
		if err = catalog.Register(typecatalog.TypeOriginPackage, typ); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "app", Package: "example.com/eligibility"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, hook, parent string
		invalid            bool
	}{
		{name: "generic root", hook: "app.Hook[" + location + ".hookEntity,github.com/viant/xdatly/handler.NoParent]"},
		{name: "promoted root", hook: "app.Wrapper"},
		{name: "wrong action", hook: "app.WrongAction[" + location + ".hookEntity,github.com/viant/xdatly/handler.NoParent]", invalid: true},
		{name: "canonical package imposter", hook: "app.Imposter", invalid: true},
		{name: "generic child outside supported scope", hook: "app.Hook[" + location + ".hookEntity," + location + ".hookParent]", parent: location + ".hookParent", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err = (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: tc.hook, Entity: location + ".hookEntity", Input: location + ".hookInput", Output: location + ".hookOutput", Parent: tc.parent, WriteEligibilityAllowed: true})
			if (err != nil) != tc.invalid {
				t.Fatalf("Compile error=%v invalid=%v", err, tc.invalid)
			}
		})
	}
}

func TestEntityHookCompilerWriteEligibilityAuthorityFailsClosed(t *testing.T) {
	location := reflect.TypeFor[hookEntity]().PkgPath()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[rootEntityHooks](), reflect.TypeFor[eligibleEntityHooks]()} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, hook, input string
		allowed, invalid  bool
	}{
		{name: "no method without new authority", hook: "rootEntityHooks"},
		{name: "eligibility without new authority", hook: "eligibleEntityHooks", input: location + ".hookInput", invalid: true},
		{name: "eligibility without canonical input", hook: "eligibleEntityHooks", allowed: true, invalid: true},
		{name: "eligibility with whitespace input", hook: "eligibleEntityHooks", input: "  ", allowed: true, invalid: true},
		{name: "explicit root authority", hook: "eligibleEntityHooks", input: location + ".hookInput", allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: tc.hook, Entity: location + ".hookEntity", Input: tc.input, Output: location + ".hookOutput", WriteEligibilityAllowed: tc.allowed})
			if (err != nil) != tc.invalid {
				t.Fatalf("Compile=%v invalid=%v", err, tc.invalid)
			}
		})
	}
}
