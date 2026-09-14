package transcribe

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	tcolumn "github.com/viant/datly/transcribe/column"
	tcompile "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

func TestCASTOverridesExistingColumnType(t *testing.T) {
	for _, test := range []struct {
		name, casts                 string
		previous                    spec.TypeRef
		nullable, pointer, conflict bool
	}{
		{name: "direct unquoted pointer", casts: "CAST(r.a AS *int)", previous: spec.TypeRef{Name: "int"}, pointer: true},
		{name: "reverse nullable pointer", casts: "CAST(r.a AS int)", previous: spec.TypeRef{Name: "int", Pointer: true}, nullable: true},
		{name: "pointer remains single", casts: "CAST(r.a AS *int)", previous: spec.TypeRef{Name: "int", Pointer: true}, nullable: true, pointer: true},
		{name: "same casts", casts: "CAST(r.a AS *int),CAST(r.a AS *int)", previous: spec.TypeRef{Name: "int"}, pointer: true},
		{name: "conflicting casts", casts: "CAST(r.a AS *int),CAST(r.a AS int)", previous: spec.TypeRef{Name: "int"}, conflict: true},
		{name: "conflict through canonical alias", casts: "CAST(r.a AS *int),CAST(r.value AS int)", previous: spec.TypeRef{Name: "int"}, conflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			column := &spec.Column{Name: "value", Source: "a", Type: test.previous, Nullable: test.nullable, Tag: `json:"custom"`}
			view := &spec.View{Name: "Records", Source: &spec.ViewSource{}, Columns: []*spec.Column{column}}
			before, _ := json.Marshal(view)
			result, err := tcompile.NewReader().Compile(tcompile.ReadInput{View: view, SQL: "SELECT r.*," + test.casts + " FROM records r"})
			after, _ := json.Marshal(view)
			if !bytes.Equal(before, after) {
				t.Fatal("CAST mutated original metadata")
			}
			if test.conflict {
				if err == nil || !strings.Contains(err.Error(), "another CAST") {
					t.Fatalf("error %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			actual := result.Columns[0]
			effective := actual.EffectiveType()
			if !actual.ExplicitType || effective.Name != "int" || effective.Pointer != test.pointer || actual.Tag != column.Tag {
				t.Fatalf("column %+v effective %+v", actual, effective)
			}
			if strings.Contains(strings.ToUpper(result.Source.SQL), "CAST") {
				t.Fatal(result.Source.SQL)
			}
			plan, err := generate.New(generate.Input{Component: &spec.Component{Name: "Records", RootView: result}}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			field, ok := plan.Views[0].Field("Value")
			want := "int"
			if test.pointer {
				want = "*int"
			}
			runtimeType, err := (xshape.Runtime{}).Type(field.Type)
			if !ok || !field.ExplicitType || field.Type != want || err != nil || (runtimeType.Kind() == reflect.Pointer) != test.pointer {
				t.Fatalf("planned/runtime field %+v %v %v", field, runtimeType, err)
			}
		})
	}
}

func TestCASTRegeneratesExactCustomizedField(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(z TEXT,a INTEGER,b TEXT)", "INSERT INTO records VALUES('z',1,'b')"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/castoverride"}).Write(t, root)
	source := &Source{Scope: "example.com/generated/records", Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: "#setting($_ = $route('/records','GET'))\nSELECT r.z,r.a,CAST(r.z AS string),CAST(r.a AS int) FROM records r"}
	run := func() *GeneratedPackage {
		copy := *source
		copy.Types = typecatalog.NewCatalog()
		compiler := NewCompiler()
		compiled, err := compiler.Compile(ctx, &copy)
		if err != nil {
			t.Fatal(err)
		}
		before, _ := json.Marshal(compiled.Component)
		input, dir, err := generationInput(root, "generated", compiled)
		if err != nil {
			t.Fatal(err)
		}
		input.SQLResources = true
		result, err := compiler.generateInputAt(ctx, root, dir, compiled, input)
		if err != nil {
			t.Fatal(err)
		}
		after, _ := json.Marshal(compiled.Component)
		if !bytes.Equal(before, after) {
			t.Fatal("generation mutated compiled metadata")
		}
		return result
	}
	initial := run()
	view := initial.Result.Plan.Views[0]
	shapePath := filepath.Join(root, "generated", view.Destination)
	initialBytes, err := os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := (xshape.SourceParser{}).ParseFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	initialField, found := view.Field("A")
	if !found || initialField.Type != "int" {
		t.Fatalf("initial int field absent: %+v", view)
	}
	for _, field := range parsed.Fields {
		if field.Owner == view.Name && len(field.Names) == 1 && field.Names[0] == "A" && field.TypeExpr != "int" {
			t.Fatalf("initial field %+v", field)
		}
	}
	// Authored tags and methods are not regenerated or adopted as trusted bytes.
	customized := strings.Replace(string(initialBytes), "type "+view.Name+" struct", "// Keep row comment\ntype "+view.Name+" struct", 1)
	customized = strings.Replace(customized, `sqlx:"a"`, `sqlx:"a" custom:"keep"`, 1)
	customized += "\n// Keep method\nfunc (r " + view.Name + ") Custom() string { return r.Z }\n"
	if err = os.WriteFile(shapePath, []byte(customized), 0644); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "generated", ".datly-gen.json")
	manifestBefore, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	source.Text = "#setting($_ = $route('/records','GET'))\nSELECT r.a,r.b,r.z,CAST(r.a AS *int),CAST(r.z AS string) FROM records r"
	updated := run()
	field, ok := updated.Result.Plan.Views[0].Field("A")
	if !ok || field.Type != "*int" || !field.ExplicitType {
		t.Fatalf("field %+v", field)
	}
	after, err := os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), "// Keep row comment") || !strings.Contains(string(after), "// Keep method") || !strings.Contains(string(after), `custom:"keep"`) {
		t.Fatalf("customized source lost: %s", after)
	}
	parsed, err = (xshape.SourceParser{}).ParseFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, field := range parsed.Fields {
		if field.Owner != view.Name {
			continue
		}
		names = append(names, field.Names...)
		if len(field.Names) == 1 && field.Names[0] == "A" && field.TypeExpr != "*int" {
			t.Fatalf("regenerated field %+v", field)
		}
	}
	if strings.Join(names, ",") != "Z,A,B" {
		t.Fatalf("field order %v", names)
	}
	manifestAfter, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var oldManifest, newManifest struct{ Fingerprints map[string]string }
	if err = json.Unmarshal(manifestBefore, &oldManifest); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(manifestAfter, &newManifest); err != nil {
		t.Fatal(err)
	}
	if oldManifest.Fingerprints[view.Destination] != newManifest.Fingerprints[view.Destination] {
		t.Fatal("customized source fingerprint was blessed")
	}
	run()
	again, err := os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, again) {
		t.Fatalf("repeated CAST rewrote source: %s", again)
	}
	consumer := strings.ReplaceAll(castOverrideConsumer, "CAST_ROW", view.Name)
	if err = os.WriteFile(filepath.Join(root, "generated", "cast_consumer_test.go"), []byte(consumer), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("compiled generated consumer: %v\n%s", err, output)
	}
	// Reverse authority must remove the pointer even with discovered nullable metadata.
	source.Text = "#setting($_ = $route('/records','GET'))\nSELECT r.a,r.b,r.z,CAST(r.a AS int),CAST(r.z AS string) FROM records r"
	run()
	parsed, err = (xshape.SourceParser{}).ParseFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range parsed.Fields {
		if field.Owner == view.Name && len(field.Names) == 1 && field.Names[0] == "A" && field.TypeExpr != "int" {
			t.Fatalf("reverse field %+v", field)
		}
	}
}

const castOverrideConsumer = `package records
import (
 "context"
 "reflect"
 "testing"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader"
)
var _ *int = CAST_ROW{}.A
func TestCASTPointerConsumer(t *testing.T) {
 ctx:=context.Background();h:=sqlite.New(t)
 if err:=h.ExecStatements(ctx,"CREATE TABLE records(z TEXT,a INTEGER,b TEXT)","INSERT INTO records VALUES('z',1,'b'),('null',NULL,'n'),('zero',0,'n')");err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
 component:=&spec.Component{Name:"Records",Key:spec.Key{Kind:spec.KindComponent,Name:"Records"},Routes:[]*spec.Route{{Method:"GET",Path:"/records"}}}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
 execution,err:=reader.NewExecution(reader.Config{Component:artifact.Component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{}),Plan:artifact.Reader,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
 actual,err:=execution.Read(ctx,&RecordsInput{},nil,nil);if err!=nil{t.Fatal(err)}
 rows:=actual.(*RecordsOutput).Data
 if len(rows)!=3{t.Fatalf("row count=%d",len(rows))}
 if rows[0].A==nil||*rows[0].A!=1||rows[1].A!=nil||rows[2].A==nil||*rows[2].A!=0||rows[0].Custom()!="z"{t.Fatalf("pointer rows lost NULL/zero distinction: %+v / %+v / %+v",*rows[0],*rows[1],*rows[2])}
}
`

// Package-compiled row types are deliberately not editable generated shapes.
type castLinkedIntRow struct{ A int }
type castLinkedPointerRow struct{ A *int }
type castLinkedIntParent struct{ Children []*castLinkedIntRow }
type castLinkedPointerParent struct{ Children []*castLinkedPointerRow }

func TestCASTRejectsUneditableLinkedFieldMismatch(t *testing.T) {
	for _, test := range []struct {
		name    string
		row     any
		failure bool
	}{
		{"compiled int", castLinkedIntRow{}, true}, {"compiled pointer", castLinkedPointerRow{}, false},
		{"nested compiled int", castLinkedIntParent{}, true}, {"nested compiled pointer", castLinkedPointerParent{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeOf(test.row))
			if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			view, err := tcompile.NewReader().Compile(tcompile.ReadInput{View: &spec.View{Name: "Linked", Source: &spec.ViewSource{}}, SQL: "SELECT r.*,CAST(r.a AS *int) FROM records r"})
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(test.name, "nested") {
				view = &spec.View{Name: "Parent", Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: view}}}
			}
			component := &spec.Component{Name: "Records", RootView: &spec.View{Name: "Root"}, Views: []*spec.View{view}}
			identity, err := view.Identity()
			if err != nil {
				t.Fatal(err)
			}
			_, err = generate.New(generate.Input{Component: component, TypeResolver: resolver, Views: generate.ViewReferences{identity: &generate.ViewReference{DescriptorKey: descriptor.Key()}}}).Plan()
			if test.failure {
				if err == nil || !strings.Contains(err.Error(), "uneditable linked field") {
					t.Fatalf("error %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			original, _ := reflect.TypeOf(test.row).FieldByName("A")
			if test.failure && !strings.HasPrefix(test.name, "nested") && original.Type.Kind() != reflect.Int {
				t.Fatal("compiled type changed")
			}
		})
	}
}

func TestCASTImportedPointerIdentity(t *testing.T) {
	const packagePath = "time"
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(time.Time{}))); err != nil {
		t.Fatal(err)
	}
	context := &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "clock", Package: packagePath}, {Alias: "same", Package: packagePath}}}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "clock", Package: packagePath}, {Alias: "same", Package: packagePath}}})
	if err != nil {
		t.Fatal(err)
	}
	original := &spec.View{Name: "Records", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "at", Type: spec.TypeRef{Name: "int"}, Nullable: true}}}
	result, err := tcompile.NewReader().Compile(tcompile.ReadInput{View: original, SQL: "SELECT r.*,CAST(r.at AS *clock.Time),CAST(r.at AS *same.Time) FROM records r", Types: resolver, TypeContext: context})
	if err != nil {
		t.Fatal(err)
	}
	actual := result.Columns[0].EffectiveType()
	if actual.Package != packagePath || actual.Name != "Time" || !actual.Pointer || original.Columns[0].Type.Name != "int" {
		t.Fatalf("type authority %+v", actual)
	}
	plan, err := generate.New(generate.Input{Component: &spec.Component{Name: "Records", RootView: result, TypeContext: context}, TypeResolver: resolver}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := plan.Views[0].Field("At")
	if !ok || field.Type != "*clock.Time" {
		t.Fatalf("field %+v", field)
	}
	runtimeType, err := (xshape.Runtime{Imports: map[string]string{"clock": packagePath}, Lookup: resolver.Type}).Type(field.Type)
	if err != nil || runtimeType != reflect.TypeOf((*time.Time)(nil)) {
		t.Fatalf("runtime type %v %v", runtimeType, err)
	}
}

func TestCASTAliasedSQLRemainsExecutable(t *testing.T) {
	const SQL = "SELECT CAST(r.a AS TEXT) AS a FROM records r"
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{{Name: "a", Type: spec.TypeRef{Name: "int"}}}}
	result, err := tcompile.NewReader().Compile(tcompile.ReadInput{View: view, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	if result.Source.SQL != SQL || result.Columns[0].ExplicitType || result.Columns[0].Type != view.Columns[0].Type {
		t.Fatalf("SQL CAST gained Go type authority: %+v", result.Columns[0])
	}
}
