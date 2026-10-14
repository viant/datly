package transcribe

import (
	"context"
	"github.com/viant/datly/spec"
	"testing"
)

func TestCompilerCanonicalOutputReload(t *testing.T) {
	for _, c := range []struct{ name, authored, kind, slot string }{
		{"empty-body", "body/", "body", "body"},
		{"same-name-body", "body/data", "body", "data"},
		{"output-view", "output/view", "output", "view"},
		{"output-status", "output/status", "output", "status"},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := &spec.Component{Parameters: []*spec.Parameter{
				{Name: "Data", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "string"},
				{Name: "Data", Source: spec.BindSource{Kind: "output", Name: c.slot}, TypeExpr: "string"},
				{Name: "Inherited", Source: spec.BindSource{Kind: "query", Name: "inherited"}, TypeExpr: "string"},
			}}
			text := `#setting($_ = $route('/records','GET'))
#define($_ = $Data<string>(body/data).Optional())
#define($_ = $Data<string>(` + c.authored + `).Output())
SELECT 1`
			compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/identity/records", Name: "records", PackageComponent: base, Text: text})
			if err != nil {
				t.Fatal(err)
			}
			if len(compiled.Component.Parameters) != 3 {
				t.Fatalf("parameters %#v", compiled.Component.Parameters)
			}
			outCount, inCount := 0, 0
			for _, p := range compiled.Component.Parameters {
				if p.Name == "Data" {
					if p.EmitOutput {
						outCount++
						if p.Source.Kind != c.kind || p.Source.Name != map[bool]string{true: "", false: c.slot}[c.name == "empty-body"] {
							t.Fatalf("authored source changed %#v", p.Source)
						}
					} else {
						inCount++
					}
				}
			}
			if outCount != 1 || inCount != 1 {
				t.Fatalf("owners out=%d in=%d", outCount, inCount)
			}
			if compiled.Component.Parameters[2].Name != "Inherited" {
				t.Fatal("inherited parameter lost")
			}
			if base.Parameters[1].Source.Kind != "output" || base.Parameters[1].EmitOutput {
				t.Fatal("package source mutated")
			}
		})
	}
}
