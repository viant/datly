package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

type linkerContractHandler struct{ input, output reflect.Type }

func (h *linkerContractHandler) InputType() reflect.Type  { return h.input }
func (h *linkerContractHandler) OutputType() reflect.Type { return h.output }
func (*linkerContractHandler) Execute(context.Context, rhandler.Invocation) (any, error) {
	return nil, nil
}

type linkerNilMap map[string]int

func (linkerNilMap) InputType() reflect.Type                                   { panic("nil map contract invoked") }
func (linkerNilMap) OutputType() reflect.Type                                  { panic("nil map contract invoked") }
func (linkerNilMap) Execute(context.Context, rhandler.Invocation) (any, error) { return nil, nil }

func TestHandlerLinkerRejectsInvalidFactoriesBeforeExecution(t *testing.T) {
	input := reflect.TypeFor[struct{}]()
	output := reflect.TypeFor[struct{ ID int }]()
	valid := &linkerContractHandler{input: input, output: output}
	cause := errors.New("factory cause")
	for _, test := range []struct {
		name                   string
		factory                func(*bool) any
		invoke, success, cause bool
	}{
		{"argument required", func(c *bool) any { return func(int) rhandler.TypedHandler { *c = true; return valid } }, false, false, false},
		{"wrong result", func(c *bool) any { return func() string { *c = true; return "wrong" } }, false, false, false},
		{"wrong second result", func(c *bool) any { return func() (rhandler.TypedHandler, int) { *c = true; return valid, 0 } }, false, false, false},
		{"nil pointer", func(c *bool) any {
			return func() rhandler.TypedHandler { *c = true; return (*linkerContractHandler)(nil) }
		}, true, false, false},
		{"nil map", func(c *bool) any { return func() rhandler.TypedHandler { *c = true; return linkerNilMap(nil) } }, true, false, false},
		{"input mismatch", func(c *bool) any {
			return func() rhandler.TypedHandler { *c = true; return &linkerContractHandler{output, output} }
		}, true, false, false},
		{"output mismatch", func(c *bool) any {
			return func() rhandler.TypedHandler { *c = true; return &linkerContractHandler{input, input} }
		}, true, false, false},
		{"factory error", func(c *bool) any { return func() (rhandler.TypedHandler, error) { *c = true; return nil, cause } }, true, false, true},
		{"factory panic", func(c *bool) any { return func() rhandler.TypedHandler { *c = true; panic(cause) } }, true, false, true},
		{"valid concrete", func(c *bool) any { return func() *linkerContractHandler { *c = true; return valid } }, true, true, false},
		{"valid error result", func(c *bool) any { return func() (rhandler.TypedHandler, error) { *c = true; return valid, nil } }, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			function, err := x.NewFunction("example.com/handlers", "Factory", test.factory(&called))
			if err != nil {
				t.Fatal(err)
			}
			registry := x.NewRegistry()
			if err := registry.RegisterFunctions(function); err != nil {
				t.Fatal(err)
			}
			linker := handlerLinker{artifact: &Artifact{inputType: input, outputType: output}, registry: registry}
			actual, err := linker.resolve(function.Key())
			if (err == nil) != test.success || called != test.invoke {
				t.Fatalf("handler=%v error=%v invoked=%v", actual, err, called)
			}
			if test.cause && !errors.Is(err, cause) {
				t.Fatalf("factory cause lost: %v", err)
			}
			if !test.success && actual != nil {
				t.Fatal("failed factory published handler")
			}
		})
	}
}

func TestHandlerLinkerPackageAuthority(t *testing.T) {
	for _, test := range []struct {
		name    string
		refs    []string
		want    string
		invalid bool
	}{
		{"component package", []string{"Factory"}, "example.com/component.Factory", false},
		{"import alias", []string{"shared.Factory"}, "example.com/shared.Factory", false},
		{"full import", []string{"example.com/shared.Factory"}, "example.com/shared.Factory", false},
		{"same canonical route factory", []string{"shared.Factory", "example.com/shared.Factory"}, "example.com/shared.Factory", false},
		{"conflicting routes", []string{"Factory", "shared.Factory"}, "", true},
		{"pointer expression", []string{"*Factory"}, "", true},
		{"generic expression", []string{"Factory[int]"}, "", true},
		{"unexported function", []string{"factory"}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Scope: "example.com/component"}, TypeContext: &spec.TypeContext{DefaultPackage: "example.com/input", Imports: []spec.ImportSpec{{Alias: "shared", Package: "example.com/shared"}}}}
			for _, name := range test.refs {
				component.Routes = append(component.Routes, &spec.Route{Handler: name})
			}
			actual, err := (handlerLinker{artifact: &Artifact{Component: component}}).reference()
			if (err != nil) != test.invalid || actual != test.want {
				t.Fatalf("reference=%q error=%v", actual, err)
			}
		})
	}
}
