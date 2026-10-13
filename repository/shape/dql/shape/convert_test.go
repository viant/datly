package shape

import (
	"reflect"
	"testing"

	"github.com/viant/datly/repository/shape/dql/ir"
	"github.com/viant/datly/repository/shape/typectx"
)

func TestFromIRToIR_RoundTripPreservesRoot(t *testing.T) {
	source := &ir.Document{Root: map[string]any{
		"Routes": []any{
			map[string]any{
				"Name":   "Route",
				"URI":    "/x",
				"Method": "GET",
				"View": map[string]any{
					"Ref": "rootView",
				},
			},
		},
		"Resource": map[string]any{
			"Views": []any{
				map[string]any{
					"Name":  "rootView",
					"Table": "T",
					"Connector": map[string]any{
						"Ref": "main",
					},
					"Template": map[string]any{
						"Source": "SELECT * FROM T",
					},
				},
			},
		},
	}}
	shapeDoc, err := FromIR(source)
	if err != nil {
		t.Fatalf("FromIR failed: %v", err)
	}
	if shapeDoc == nil || len(shapeDoc.Routes) != 1 || shapeDoc.Resource == nil || len(shapeDoc.Resource.Views) != 1 {
		t.Fatalf("unexpected shape projection: %+v", shapeDoc)
	}
	target, err := ToIR(shapeDoc)
	if err != nil {
		t.Fatalf("ToIR failed: %v", err)
	}
	if !reflect.DeepEqual(source.Root, target.Root) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestToIR_FromIR_TypeContextRoundTrip(t *testing.T) {
	doc := &Document{
		Root: map[string]any{
			"Routes":   []any{},
			"Resource": map[string]any{},
		},
		TypeContext: &typectx.Context{
			DefaultPackage: "mdp/performance",
			Imports: []typectx.Import{
				{Alias: "perf", Package: "github.com/acme/mdp/performance"},
			},
		},
	}
	irDoc, err := ToIR(doc)
	if err != nil {
		t.Fatalf("ToIR failed: %v", err)
	}
	shapeDoc, err := FromIR(irDoc)
	if err != nil {
		t.Fatalf("FromIR failed: %v", err)
	}
	if shapeDoc.TypeContext == nil {
		t.Fatalf("expected type context")
	}
	if shapeDoc.TypeContext.DefaultPackage != "mdp/performance" {
		t.Fatalf("unexpected default package: %s", shapeDoc.TypeContext.DefaultPackage)
	}
	if len(shapeDoc.TypeContext.Imports) != 1 {
		t.Fatalf("unexpected imports count: %d", len(shapeDoc.TypeContext.Imports))
	}
}

func TestToIR_FromIR_TypeResolutionsRoundTrip(t *testing.T) {
	doc := &Document{
		Root: map[string]any{
			"Routes":   []any{},
			"Resource": map[string]any{},
		},
		TypeResolutions: []typectx.Resolution{
			{
				Expression:  "Order",
				Target:      "main.ID",
				ResolvedKey: "github.com/acme/mdp/performance.Order",
				MatchKind:   "default_package",
				Provenance: typectx.Provenance{
					Package: "github.com/acme/mdp/performance",
					File:    "/repo/mdp/performance/order.go",
					Kind:    "resource_type",
				},
			},
		},
	}
	irDoc, err := ToIR(doc)
	if err != nil {
		t.Fatalf("ToIR failed: %v", err)
	}
	shapeDoc, err := FromIR(irDoc)
	if err != nil {
		t.Fatalf("FromIR failed: %v", err)
	}
	if len(shapeDoc.TypeResolutions) != 1 {
		t.Fatalf("unexpected type resolutions count: %d", len(shapeDoc.TypeResolutions))
	}
	got := shapeDoc.TypeResolutions[0]
	if got.ResolvedKey != "github.com/acme/mdp/performance.Order" || got.Provenance.Kind != "resource_type" {
		t.Fatalf("unexpected resolution: %+v", got)
	}
}
