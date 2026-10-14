package generate

import (
	"go/parser"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	tpl "github.com/viant/datly/sql/template"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"github.com/viant/x/syntetic/model"
	legacy "github.com/viant/xdatly/handler/response"
	response "github.com/viant/xdatly/response"
)

type discoveryEmbeddingAccount struct{ ID int }
type DiscoveryStatusAlias = response.Status
type discoveryEmbeddingModernValue struct {
	response.Status `json:",omitempty"`
	Account         *discoveryEmbeddingAccount `json:"account"`
}
type discoveryEmbeddingModernPointer struct {
	*response.Status
	Account *discoveryEmbeddingAccount
}
type discoveryEmbeddingLegacyValue struct {
	legacy.Status
	Account *discoveryEmbeddingAccount
}
type discoveryEmbeddingAlias struct {
	DiscoveryStatusAlias
	Account *discoveryEmbeddingAccount
}
type discoveryEmbeddingShadow struct {
	response.Status
	Message string
	Account *discoveryEmbeddingAccount
}
type discoveryEmbeddingNested struct{ legacy.Status }
type DiscoveryNested = discoveryEmbeddingNested

type discoveryEmbeddingNestedReply struct {
	DiscoveryNested
	Account *discoveryEmbeddingAccount
}
type DiscoveryEmbeddingLeft struct{ Code string }
type DiscoveryEmbeddingRight struct{ Code string }
type discoveryEmbeddingConflict struct {
	DiscoveryEmbeddingLeft
	DiscoveryEmbeddingRight
	Account *discoveryEmbeddingAccount
}

func embeddingDiscoveryGenerator(t *testing.T, definitions map[string]string, linked bool) *Generator {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []*x.Type{x.NewType(reflect.TypeFor[legacy.Status]()), x.NewType(reflect.TypeFor[discoveryEmbeddingAccount](), x.WithName("Account"), x.WithPkgPath("example.com/models")), x.NewType(reflect.TypeFor[discoveryEmbeddingNested](), x.WithName("DiscoveryNested"), x.WithPkgPath("example.com/models")), x.NewType(reflect.TypeFor[DiscoveryEmbeddingLeft](), x.WithName("DiscoveryEmbeddingLeft"), x.WithPkgPath("example.com/models")), x.NewType(reflect.TypeFor[DiscoveryEmbeddingRight](), x.WithName("DiscoveryEmbeddingRight"), x.WithPkgPath("example.com/models"))} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, typ); err != nil {
			t.Fatal(err)
		}
	}
	for name, definition := range definitions {
		expression, err := parser.ParseExpr(definition)
		if err != nil {
			t.Fatal(err)
		}
		descriptor := namedDiscoveryDeclaration(t, name, definition)
		descriptor.SynteticType.TypeSpec.Type = expression
		descriptor.SynteticType.Imports = map[string]*model.ImportRef{"response": {Path: "github.com/viant/xdatly/response"}, "legacy": {Path: "github.com/viant/xdatly/handler/response"}}
		if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
			t.Fatal(err)
		}
	}
	if linked {
		if err := catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[discoveryEmbeddingModernValue](), x.WithName("Reply"), x.WithPkgPath("example.com/models"))); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "models", Package: "example.com/models"}}})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Name: "Records", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "models", Package: "example.com/models"}}}, Parameters: []*spec.Parameter{{Name: "Accounts", TypeExpr: "*models.Reply", Source: spec.BindSource{Kind: "component", Name: "GET:/accounts"}}}}
	return New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated"})
}
func embeddingDiscoveryReply(t *testing.T, g *Generator) reflect.Type {
	t.Helper()
	input, err := g.ColumnDiscoveryInputType()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := input.FieldByName("Accounts")
	if !ok || field.Type.Kind() != reflect.Pointer {
		t.Fatalf("Accounts=%+v", field)
	}
	return field.Type.Elem()
}
func embeddingSurfaceEqual(t *testing.T, actual, expected reflect.Type) {
	t.Helper()
	if actual.NumField() != expected.NumField() {
		t.Fatalf("direct declarations=%v vs compiled=%v", actual, expected)
	}
	for n := 0; n < expected.NumField(); n++ {
		a, b := actual.Field(n), expected.Field(n)
		if a.Name != b.Name || a.Type != b.Type || a.Tag != b.Tag || a.Anonymous != b.Anonymous {
			t.Fatalf("declaration=%+v expected=%+v", a, b)
		}
	}
	for _, name := range []string{"Status", "Message", "Error", "Errors", "Warning", "Extras", "Code", "Account"} {
		a, ok := actual.FieldByName(name)
		b, want := expected.FieldByName(name)
		if ok != want {
			t.Fatalf("%s visibility=%v vs compiled=%v", name, ok, want)
		}
		if ok && (a.Type != b.Type || a.Anonymous != b.Anonymous || !reflect.DeepEqual(a.Index, b.Index)) {
			t.Fatalf("%s selector=%+v expected=%+v", name, a, b)
		}
	}
}
func TestColumnDiscoveryAnonymousStatusPreservesDeclarationTopology(t *testing.T) {
	for _, tc := range []struct {
		name, definition string
		expected         reflect.Type
		aliases          map[string]string
		template         bool
	}{
		{"modern value and tags", "struct {response.Status `json:\",omitempty\"`;Account *Account `json:\"account\"`}", reflect.TypeFor[discoveryEmbeddingModernValue](), nil, true},
		{"modern pointer", "struct {*response.Status;Account *Account}", reflect.TypeFor[discoveryEmbeddingModernPointer](), nil, true},
		{"nominal legacy", "struct {legacy.Status;Account *Account}", reflect.TypeFor[discoveryEmbeddingLegacyValue](), nil, true},
		{"alias", "struct {DiscoveryStatusAlias;Account *Account}", reflect.TypeFor[discoveryEmbeddingAlias](), map[string]string{"DiscoveryStatusAlias": "response.Status"}, true},
		{"direct shadow", "struct {response.Status;Message string;Account *Account}", reflect.TypeFor[discoveryEmbeddingShadow](), nil, false},
		{"compiled nested named holder", "struct {DiscoveryNested;Account *Account}", reflect.TypeFor[discoveryEmbeddingNestedReply](), nil, true},
		{"equal depth conflict", "struct {DiscoveryEmbeddingLeft;DiscoveryEmbeddingRight;Account *Account}", reflect.TypeFor[discoveryEmbeddingConflict](), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definitions := map[string]string{"Reply": tc.definition}
			for k, v := range tc.aliases {
				definitions[k] = v
			}
			g := embeddingDiscoveryGenerator(t, definitions, false)
			actual := embeddingDiscoveryReply(t, g)
			embeddingSurfaceEqual(t, actual, tc.expected)
			if _, err := g.RuntimeInputType(); err == nil {
				t.Fatal("source-only temporary discovery became compiled runtime authority")
			}
			if tc.template {
				input, err := g.ColumnDiscoveryInputType()
				if err != nil {
					t.Fatal(err)
				}
				if _, err = (tpl.Compiler{Source: "SELECT ID FROM records WHERE $Accounts.Account.ID <= 0", InputType: input, Variables: []tpl.Variable{{Name: "Accounts", FieldIndex: []int{0}}}}).Compile(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestColumnDiscoveryOmittedPrivateRootKeepsExportedProjection(t *testing.T) {
	g := embeddingDiscoveryGenerator(t, map[string]string{"Reply": "struct {hidden;Account *Account}", "hidden": "struct {PublicID int;private string}"}, false)
	actual := embeddingDiscoveryReply(t, g)
	if _, ok := actual.FieldByName("hidden"); ok {
		t.Fatal("private embedding newly reconstructed")
	}
	if _, ok := actual.FieldByName("private"); ok {
		t.Fatal("private field newly exposed")
	}
	public, ok := actual.FieldByName("PublicID")
	if !ok || public.Type != reflect.TypeFor[int]() || len(public.Index) != 1 {
		t.Fatalf("existing exported projection=%+v", public)
	}
	if _, err := g.RuntimeInputType(); err == nil {
		t.Fatal("source-only private projection became runtime authority")
	}
}
func TestColumnDiscoveryEmbeddingKeepsCompiledFastPath(t *testing.T) {
	g := embeddingDiscoveryGenerator(t, map[string]string{"Reply": "struct {response.Status `json:\",omitempty\"`;Account *Account `json:\"account\"`}"}, true)
	for _, build := range []func() (reflect.Type, error){g.ColumnDiscoveryInputType, g.RuntimeInputType} {
		input, err := build()
		if err != nil {
			t.Fatal(err)
		}
		field, _ := input.FieldByName("Accounts")
		if field.Type != reflect.TypeFor[*discoveryEmbeddingModernValue]() {
			t.Fatalf("compiled exact owner replaced: %v", field.Type)
		}
	}
}
func TestColumnDiscoveryEmbeddingRejectsUnsupportedSourceScopes(t *testing.T) {
	for _, tc := range []struct{ name, definition, diagnostic string }{
		{"recursive", "struct {Next *Reply}", "recursive"},
		{"nonempty interface", "struct {Other interface {Read() error}}", `named type "interface{ Read() error }" was not found`},
		{"anonymous unnamed type", "struct {struct {ID int}}", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "anonymous unnamed type" {
				if _, err := parser.ParseExpr(tc.definition); err == nil {
					t.Fatal("invalid anonymous declaration accepted")
				}
				return
			}
			g := embeddingDiscoveryGenerator(t, map[string]string{"Reply": tc.definition}, false)
			if _, err := g.ColumnDiscoveryInputType(); err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("unsupported source error=%v", err)
			}
		})
	}
	var nilGenerator *Generator
	if _, err := nilGenerator.ColumnDiscoveryInputType(); err == nil {
		t.Fatal("nil generator accepted")
	}
}
