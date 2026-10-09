package transcribe

import (
	"go/ast"
	"go/parser"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"github.com/viant/x/syntetic/model"
)

func TestCurrentFieldAuthorityPreservesCanonicalNames(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		fields                 currentFieldAuthorities
		query, want, errorText string
	}{
		{"exact initial acronym", currentFieldAuthorities{{name: "VAdtagUrl", expression: "*string"}}, "VAdtagUrl", "VAdtagUrl", ""},
		{"SQL initial acronym", currentFieldAuthorities{{name: "VAdtagUrl", expression: "*string"}}, "V_ADTAG_URL", "VAdtagUrl", ""},
		{"single letter prefix", currentFieldAuthorities{{name: "NAssetType", expression: "*int"}}, "N_ASSET_TYPE", "NAssetType", ""},
		{"legacy acronym spelling", currentFieldAuthorities{{name: "TenantID", expression: "int64"}}, "TenantId", "TenantID", ""},
		{"exact structural precedence", currentFieldAuthorities{{name: "TenantID"}, {name: "TenantId"}}, "TenantID", "TenantID", ""},
		{"exact prefix", currentFieldAuthorities{{name: "VAdtagUrl"}, {name: "VaDtagUrl"}}, "VAdtagUrl", "VAdtagUrl", ""},
		{"exact alternate prefix", currentFieldAuthorities{{name: "VAdtagUrl"}, {name: "VaDtagUrl"}}, "VaDtagUrl", "VaDtagUrl", ""},
		{"ambiguous normalized spelling", currentFieldAuthorities{{name: "TenantID"}, {name: "TenantId"}}, "tenant_id", "", "ambiguous canonical field"},
		{"missing", currentFieldAuthorities{{name: "VAdtagUrl"}}, "Missing", "", "was not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fields.resolve(tc.query)
			if tc.errorText != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errorText) {
					t.Fatalf("resolve = %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.name != tc.want {
				t.Fatalf("resolve = %+v, %v; want %s", got, err, tc.want)
			}
		})
	}
}

func TestCurrentProjectionPreservesCanonicalPrefix(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(map[bool]string{false: "generated", true: "linked"}[linked], func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			generated := &gen.Plan{}
			for _, name := range []string{"Entity", "Previous"} {
				if !linked {
					generated.Views = append(generated.Views, gen.ViewPlan{Name: name, Type: name, Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "VAdtagUrl", Type: "*string"}}})
					continue
				}
				expression, err := parser.ParseExpr("struct { VAdtagUrl *string }")
				if err != nil {
					t.Fatal(err)
				}
				if err = catalog.Register(typecatalog.TypeOriginPackage, &x.Type{Name: name, PkgPath: "example.com/generated", SynteticType: &model.Type{Name: name, PkgPath: "example.com/generated", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(name), Type: expression}}}); err != nil {
					t.Fatal(err)
				}
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/generated"})
			if err != nil {
				t.Fatal(err)
			}
			generation := newHandlerGeneration(nil, &gen.Input{TargetPackage: "example.com/generated", TypeResolver: resolver}, Options{})
			record := &plan.RecordPlan{Identity: "entity", Cardinality: spec.CardinalityMany, Current: &plan.CurrentPlan{ViewIdentity: "previous", Fields: []plan.CurrentField{{Current: plan.FieldRef{Field: "V_ADTAG_URL"}, Entity: plan.FieldRef{Field: "V_ADTAG_URL"}}}}}
			if err = generation.refineCurrentProjection(record, generated, "[]*Entity", "[]*Previous"); err != nil {
				t.Fatal(err)
			}
			field := record.Current.Fields[0]
			if field.Current.Field != "VAdtagUrl" || field.Entity.Field != "VAdtagUrl" || field.Conversion != plan.LinkDirect || field.Entity.Type.Name != "*string" {
				t.Fatalf("projection = %+v", field)
			}
		})
	}
}

func TestMutationLinkPreservesCanonicalPrefix(t *testing.T) {
	generation := newHandlerGeneration(nil, &gen.Input{}, Options{})
	links := []plan.KeyLink{{Parent: plan.KeyPart{Field: "V_PARENT_ID", Source: "V_PARENT_ID"}, Child: plan.KeyPart{Field: "V_PARENT_ID", Source: "V_PARENT_ID"}}}
	got, err := generation.refineMutationLinks(links, currentFieldAuthorities{{name: "VParentId", expression: "int64"}}, currentFieldAuthorities{{name: "VParentId", expression: "*int64"}})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Parent.Field != "VParentId" || got[0].Child.Field != "VParentId" || got[0].Conversion != plan.LinkAddress {
		t.Fatalf("links = %+v", got)
	}
	if links[0].Parent.Field != "V_PARENT_ID" {
		t.Fatal("authored links changed")
	}
}

func TestCurrentProjectionRefinesFinalCanonicalFields(t *testing.T) {
	for _, tc := range []struct {
		name       string
		linked     bool
		from, to   string
		conversion plan.LinkConversion
		invalid    bool
	}{
		{"generated direct", false, "int64", "int64", plan.LinkDirect, false},
		{"generated address", false, "int64", "*int64", plan.LinkAddress, false},
		{"generated dereference", false, "*int64", "int64", plan.LinkDereference, false},
		{"generated incompatible", false, "int64", "string", "", true},
		{"linked synthetic", true, "int64", "*int64", plan.LinkAddress, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			generated := &gen.Plan{Views: []gen.ViewPlan{{Name: "Entity", Type: "Entity", Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "TenantID", Type: tc.to}}}, {Name: "Previous", Type: "Previous", Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "LoadedTenantID", Type: tc.from}}}}}
			if tc.linked {
				for _, shape := range []struct{ name, field, typ string }{{"Entity", "TenantID", tc.to}, {"Previous", "LoadedTenantID", tc.from}} {
					expression, err := parser.ParseExpr("struct { " + shape.field + " " + shape.typ + " }")
					if err != nil {
						t.Fatal(err)
					}
					if err = catalog.Register(typecatalog.TypeOriginPackage, &x.Type{Name: shape.name, PkgPath: "example.com/generated", SynteticType: &model.Type{Name: shape.name, PkgPath: "example.com/generated", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(shape.name), Type: expression}}}); err != nil {
						t.Fatal(err)
					}
				}
				generated.Views = nil
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/generated"})
			if err != nil {
				t.Fatal(err)
			}
			generation := newHandlerGeneration(nil, &gen.Input{TargetPackage: "example.com/generated", TypeResolver: resolver}, Options{})
			original := plan.CurrentField{Current: plan.FieldRef{Field: "LoadedTenantId", Source: "tenant_id"}, Entity: plan.FieldRef{Field: "TenantId", Source: "tenant_id"}}
			record := &plan.RecordPlan{Identity: "entity", Cardinality: spec.CardinalityMany, Current: &plan.CurrentPlan{ViewIdentity: "previous", Fields: []plan.CurrentField{original}}}
			err = generation.refineCurrentProjection(record, generated, "[]*Entity", "[]*Previous")
			if (err != nil) != tc.invalid {
				t.Fatalf("refine=%v", err)
			}
			if tc.invalid {
				if record.Current.Fields[0] != original {
					t.Fatal("failed refinement partially published")
				}
				return
			}
			field := record.Current.Fields[0]
			if field.Current.Field != "LoadedTenantID" || field.Entity.Field != "TenantID" || field.Current.Type.Name != tc.from || field.Entity.Type.Name != tc.to || field.Conversion != tc.conversion {
				t.Fatalf("field=%+v", field)
			}
		})
	}
}

func TestCurrentIdentityAndSelfRefinementIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name, key, holder string
		fail              bool
	}{{"canonical", "LoadedTenantId", "ChildRows", false}, {"missing key", "Missing", "ChildRows", true}, {"missing holder", "LoadedTenantId", "Missing", true}} {
		t.Run(tc.name, func(t *testing.T) {
			generated := &gen.Plan{Views: []gen.ViewPlan{{Name: "Entity", Type: "Entity", Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "TenantID", Type: "int64"}}}, {Name: "Previous", Type: "Previous", Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "LoadedTenantID", Type: "int64"}, {Name: "ChildRows", Type: "[]*Previous"}}}}}
			current := &plan.CurrentPlan{ViewIdentity: "previous", Keys: []plan.KeyPart{{Field: tc.key, Source: "tenant_id"}}, Self: []plan.FieldRef{{Field: tc.holder}}, Fields: []plan.CurrentField{{Current: plan.FieldRef{Field: "LoadedTenantId"}, Entity: plan.FieldRef{Field: "TenantId"}}}}
			record := &plan.RecordPlan{Identity: "entity", Cardinality: spec.CardinalityMany, Current: current}
			before := (&plan.Plan{Root: record}).Clone().Root.Current
			generation := newHandlerGeneration(nil, &gen.Input{TargetPackage: "example.com/generated"}, Options{})
			err := generation.refineCurrentProjection(record, generated, "[]*Entity", "[]*Previous")
			if (err != nil) != tc.fail {
				t.Fatalf("refine %v", err)
			}
			if tc.fail {
				if !reflect.DeepEqual(before, record.Current) {
					t.Fatal("failed refinement mutated metadata")
				}
				return
			}
			if current.Keys[0].Field != "LoadedTenantID" || current.Keys[0].Type.Name != "int64" || current.Self[0].Field != "ChildRows" || current.Self[0].Type.Name != "[]*example.com/generated.Previous" {
				t.Fatalf("canonical authority %+v %+v", current.Keys, current.Self)
			}
		})
	}
}
