package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestPlanCanonicalTypeUsesEmittedImportAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, expression, want string
		imports                []spec.ImportSpec
		invalid                bool
	}{
		{"builtin", "string", "string", []spec.ImportSpec{{Package: "time"}}, false},
		{"implicit", "*time.Time", "*time.Time", []spec.ImportSpec{{Package: "time"}}, false},
		{"explicit", "[]clock.Time", "[]time.Time", []spec.ImportSpec{{Alias: "clock", Package: "time"}}, false},
		{"local", "*Record", "*example.com/generated.Record", nil, false},
		{"blank import", "int", "int", []spec.ImportSpec{{Alias: "_", Package: "example.com/driver"}}, false},
		{"empty package", "Record", "", []spec.ImportSpec{{}}, true},
		{"whitespace package", "Record", "", []spec.ImportSpec{{Package: "  "}}, true},
		{"conflict", "model.Value", "", []spec.ImportSpec{{Alias: "model", Package: "example.com/one"}, {Alias: "model", Package: "example.com/two"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := (&Plan{Imports: tc.imports}).CanonicalType("example.com/generated", tc.expression)
			if (err != nil) != tc.invalid || !tc.invalid && actual != tc.want {
				t.Fatalf("canonical=%s err=%v want=%s", actual, err, tc.want)
			}
		})
	}
}

func TestPlanCanonicalTypeActualLocalPackageName(t *testing.T) {
	for _, tc := range []struct {
		name, destination, local, want string
		imports                        []spec.ImportSpec
	}{
		{"actual package alias", "example.com/generated", "patch", "[]*example.com/generated.Creative", nil},
		{"explicit import precedence", "example.com/generated", "patch", "[]*example.com/foreign.Creative", []spec.ImportSpec{{Alias: "patch", Package: "example.com/foreign"}}},
		{"foreign destination", "example.com/other", "patch", "[]*patch.Creative", nil},
		{"unknown actual package", "example.com/generated", "", "[]*patch.Creative", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Plan{Package: "example.com/generated", GoPackage: tc.local, Imports: tc.imports}
			got, err := p.CanonicalType(tc.destination, "[]*patch.Creative")
			if err != nil || got != tc.want {
				t.Fatalf("canonical=%s err=%v want=%s", got, err, tc.want)
			}
		})
	}
}
