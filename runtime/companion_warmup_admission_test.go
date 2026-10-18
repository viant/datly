package runtime

import (
	"context"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	handlerexec "github.com/viant/xdatly/handler/exec"
	"reflect"
	"testing"
)

type warmupAdmissionReader struct{ targets []dexec.ReaderWarmupTarget }

func (*warmupAdmissionReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	panic("admission must not execute reader")
}
func (r *warmupAdmissionReader) WarmupTargets() []dexec.ReaderWarmupTarget { return r.targets }
func TestCanonicalCompanionWarmupAdmission(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	for _, parameter := range component.Parameters {
		if parameter.Name == "ID" {
			required := true
			parameter.Required = &required
		}
	}
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
	prepared, err := route.WithOptionalBindings("ID")
	if err != nil {
		t.Fatal(err)
	}
	if prepared == route {
		t.Fatal("required input was not detached")
	}
	policy := &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "ID"}
	reader := &warmupAdmissionReader{targets: []dexec.ReaderWarmupTarget{{Settings: policy}}}
	registered := &RegisteredComponent{Component: component, Input: compiled.Input, Reader: reader}
	for _, phase := range []handlerexec.WarmupPhase{handlerexec.WarmupPhasePrepare, handlerexec.WarmupPhaseFill} {
		ctx := handlerexec.WithCacheWarmup(t.Context(), phase)
		request := dexec.ComponentRequest{WarmupOmitInput: []string{"ID"}, PrepareQuery: phase == handlerexec.WarmupPhasePrepare}
		if phase == handlerexec.WarmupPhaseFill {
			request.Warmup = &dexec.ReaderWarmupRequest{Settings: policy}
		}
		if !canonicalCompanionWarmup(ctx, registered, request, route, prepared) {
			t.Fatal("exact optional root request rejected")
		}
		for name, change := range map[string]func(*dexec.ComponentRequest){
			"foreign alias":    func(r *dexec.ComponentRequest) { r.WarmupOmitInput = []string{"other"} },
			"multiple aliases": func(r *dexec.ComponentRequest) { r.WarmupOmitInput = []string{"ID", "other"} },
			"input":            func(r *dexec.ComponentRequest) { r.Input = &fixture.Input{} },
			"dry run":          func(r *dexec.ComponentRequest) { r.DryRun = true },
			"bound output":     func(r *dexec.ComponentRequest) { r.BindingOutput = new(int) },
			"phase mismatch":   func(r *dexec.ComponentRequest) { r.PrepareQuery = !r.PrepareQuery },
		} {
			t.Run(name, func(t *testing.T) {
				bad := request
				change(&bad)
				if canonicalCompanionWarmup(ctx, registered, bad, route, prepared) {
					t.Fatal("unsupported request admitted")
				}
			})
		}
		if canonicalCompanionWarmup(t.Context(), registered, request, route, prepared) {
			t.Fatal("untrusted context admitted")
		}
		reader.targets = append(reader.targets, dexec.ReaderWarmupTarget{View: "relation", Settings: policy})
		if canonicalCompanionWarmup(ctx, registered, request, route, prepared) {
			t.Fatal("relation inventory admitted")
		}
		reader.targets = reader.targets[:1]
		reader.targets = append(reader.targets, dexec.ReaderWarmupTarget{Settings: policy})
		if canonicalCompanionWarmup(ctx, registered, request, route, prepared) {
			t.Fatal("ambiguous root inventory admitted")
		}
		reader.targets = reader.targets[:1]
	}
}
