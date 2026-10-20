package compiler

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestCodecSourceCanonicalIdentityAndLegacyNames(t *testing.T) {
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "json", Package: "encoding/json"}}})
	if err != nil {
		t.Fatal(err)
	}
	rejected := errors.New("lookup rejected")
	reject := func(string) (reflect.Type, error) { return nil, rejected }
	for _, tc := range []struct {
		name, expression string
		lookup           func(string) (reflect.Type, error)
		want             reflect.Type
		wantError        string
		sentinel         error
	}{
		{name: "canonical", expression: "encoding/json.RawMessage", lookup: resolver.Type, want: reflect.TypeFor[json.RawMessage]()},
		{name: "canonical pointer slice", expression: "[]*encoding/json.RawMessage", lookup: resolver.Type, want: reflect.TypeFor[[]*json.RawMessage]()},
		{name: "canonical pointer", expression: "*encoding/json.RawMessage", lookup: resolver.Type, want: reflect.TypeFor[*json.RawMessage]()},
		{name: "short json nil lookup", expression: "json.RawMessage", want: reflect.TypeFor[json.RawMessage]()},
		{name: "short json rejecting lookup", expression: "json.RawMessage", lookup: reject, want: reflect.TypeFor[json.RawMessage]()},
		{name: "short time nil lookup", expression: "time.Time", want: reflect.TypeFor[time.Time]()},
		{name: "short time rejecting lookup", expression: "[]*time.Time", lookup: reject, want: reflect.TypeFor[[]*time.Time]()},
		{name: "canonical lookup required", expression: "encoding/json.RawMessage", wantError: "requires component type lookup"},
		{name: "canonical lookup rejection", expression: "encoding/json.RawMessage", lookup: reject, sentinel: rejected},
		{name: "canonical unresolved", expression: "example.com/missing.Value", lookup: resolver.Type, wantError: "was not resolved"},
		{name: "malformed", expression: "[]*", lookup: resolver.Type, wantError: "source type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type input struct{ Value *codecPathContext }
			param := &spec.Parameter{Name: "Value", TypeExpr: tc.expression, OutputTypeExpr: "*codecPathContext", Source: spec.BindSource{Kind: "body", Name: "data"}, Codec: &spec.Codec{Body: "Decode"}}
			factory := &transportCodecFactory{codec: &transportCodec{}}
			_, err := (ParamCodecCompiler{Component: &spec.Component{Parameters: []*spec.Parameter{param}}, InputType: reflect.TypeFor[input](), Factory: factory, LookupType: tc.lookup}).Build()
			if tc.sentinel != nil {
				if !errors.Is(err, tc.sentinel) {
					t.Fatalf("lost lookup error: %v", err)
				}
				return
			}
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("want %s, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if factory.config.SourceType != tc.want || factory.config.DestinationType != reflect.TypeFor[*codecPathContext]() {
				t.Fatalf("source/destination changed: %+v", factory.config)
			}
			if param.TypeExpr != tc.expression || param.OutputTypeExpr != "*codecPathContext" {
				t.Fatal("authored codec contract mutated")
			}
		})
	}
}
