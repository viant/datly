package transcribe

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type scalarBindingHook int
type sliceBindingHook []string
type plainBindingHook struct{ Count int }
type injectedBindingHook struct {
	Input any `bind:"kind=input"`
}

func TestEntityHookBindingMetadataAtOrchestrationBoundary(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	for _, value := range []any{scalarBindingHook(0), sliceBindingHook{}, plainBindingHook{}, injectedBindingHook{}} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(value))); err != nil {
			t.Fatal(err)
		}
	}
	location := reflect.TypeOf(plainBindingHook{}).PkgPath()
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location})
	if err != nil {
		t.Fatal(err)
	}
	compilation := &entityHookCompilation{generation: &handlerGeneration{input: &gen.Input{TypeResolver: resolver}}}
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"scalarBindingHook", false}, {"sliceBindingHook", false}, {"plainBindingHook", false}, {"injectedBindingHook", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := compilation.bindingRequired(spec.TypeRef{Package: location, Name: tc.name})
			if err != nil || got != tc.want {
				t.Fatalf("binding=%v error=%v", got, err)
			}
		})
	}
	if _, err := compilation.bindingRequired(spec.TypeRef{Name: "MissingHook"}); err == nil {
		t.Fatal("missing hook accepted")
	}
	if _, err := (&entityHookCompilation{}).bindingRequired(spec.TypeRef{}); err == nil {
		t.Fatal("missing authority accepted")
	}
}

func TestEntityLifecycleScaffoldRequiresDestinationAuthority(t *testing.T) {
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/source"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, defaultPackage string
		enabled              bool
	}{
		{"foreign default", "example.com/authored", true},
		{"scaffold disabled", "example.com/generated", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &gen.Input{TypeResolver: resolver, TargetPackage: "example.com/generated", Component: &spec.Component{TypeContext: &spec.TypeContext{DefaultPackage: tc.defaultPackage}}}
			generation := &handlerGeneration{input: input, options: Options{Handler: HandlerOptions{Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Scaffold: tc.enabled}}}}
			_, scaffold, err := (&entityHookCompilation{generation: generation}).compileHook(compiler.EntityHookRequest{Hook: "ExplicitRules", Entity: "example.com/generated.Order"})
			if err == nil || scaffold {
				t.Fatalf("invalid declaration acquired scaffold: %v %v", scaffold, err)
			}
		})
	}
}
