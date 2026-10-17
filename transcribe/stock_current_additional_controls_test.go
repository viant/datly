package transcribe

import (
	"bytes"
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestStockCurrentMissingNestedAuthorityAndNonstandardReadonly(t *testing.T) {
	for name, want := range map[string]string{
		"authority-missing-nested-aux": `auxiliary lookup view::Chain|namespace:Chain with nested relations requires explicit authored current authority`,
		"authority-nonstandard-aux":    `auxiliary lookup view::Carrier|namespace:Carrier with nested relations requires explicit authored current authority`,
	} {
		t.Run(name, func(t *testing.T) {
			source, root := stockExpandedCurrentFixture(t, name)
			before := source.Text
			_, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("intended authority stage %q got %v", want, err)
			}
			if source.Text != before {
				t.Fatal("source mutation")
			}
			stockCurrentNoEmission(t, root)
			t.Logf("unchanged required authority: %v", err)
		})
	}
}

func TestStockCurrentAbsentPhysicalChildUsesImmediateAuxCurrent(t *testing.T) {
	for _, name := range []string{"absent-physical-current", "authority-readonly-child"} {
		t.Run(name, func(t *testing.T) {
			source, root := stockExpandedCurrentFixture(t, name)
			stockAssertCompleteAuthority(t, source, root, map[string]bool{"Probe": false, "Carrier": true, "Children": false}, map[string]string{"Carrier": "Probe", "Children": "Carrier"})
			generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, f := range generated.Result.Files {
				if strings.HasSuffix(f.Path, "current_children.sql") {
					b, e := os.ReadFile(f.Path)
					if e != nil {
						t.Fatal(e)
					}
					if !strings.Contains(string(b), `$Unsafe.ProjectCurrentChildrenParentKeys($CurrentCarrier)`) || strings.Contains(string(b), `ParentKeys($CurrentProbe)`) {
						t.Fatalf("child scope not immediate auxiliary: %s", b)
					}
					found = true
					t.Logf("derived child authorized by immediate CurrentCarrier: %s", b)
				}
			}
			if !found {
				t.Fatal("missing derived CurrentChildren SQL")
			}
		})
	}
}

func TestStockCurrentPackageDiscoveryWithRuntimeKeyProjection(t *testing.T) {
	source, root := stockExpandedCurrentFixture(t, "absent-physical-current")
	source.Text = strings.Replace(source.Text, "$CurrentProbe<?>(view/CurrentProbe).Cardinality('Many')", "$CurrentProbe<?>(view/CurrentProbe).Cardinality('Many').WithTypeName('CurrentProbeView')", 1)
	source.Text = strings.Replace(source.Text, "$CurrentCarrier<?>(view/CurrentCarrier).Cardinality('Many')", "$CurrentCarrier<?>(view/CurrentCarrier).Cardinality('Many').WithTypeName('CurrentCarrierView')", 1)
	source.Text = strings.Replace(source.Text, "(view/CurrentCarrier)", "(view/CurCarrier)", 1)
	ctx := context.Background()
	_, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	text := "#import('generated','github.com/viant/datly/stockcurrentfixture/generated')\n" + source.Text
	writeSourceFile(t, root, "source/probe.dql", text)
	discovery := &Discovery{BaseDir: root, Include: []string{"github.com/viant/datly/stockcurrentfixture/source"}, SourceFile: "probe.dql", Connector: "main", ColumnRefiner: source.ColumnRefiner}
	snapshots := map[string][]byte{}
	for generation := 2; generation <= 3; generation++ {
		project, err := discovery.Compile(ctx)
		if err != nil {
			t.Fatalf("discovery generation %d: %v", generation, err)
		}
		if len(project.Components) != 1 {
			t.Fatalf("components: %d", len(project.Components))
		}
		compiled := project.Components[0]
		if len(compiled.Source.selfGeneratedInputViews) != 3 || len(compiled.Source.inheritedGeneratedCurrents) != 1 {
			t.Fatalf("generation ownership proof: own=%v inherited=%v", compiled.Source.selfGeneratedInputViews, compiled.Source.inheritedGeneratedCurrents)
		}

		for _, view := range compiled.Component.Views {
			if view.Name == "CurrentProbe" && (view.Source == nil || !strings.Contains(strings.ReplaceAll(view.Source.SQL, " ", ""), "id=1")) {
				t.Fatalf("authored root Current filter changed: %+v", view.Source)
			}
			if (view.Name == "CurrentCarrier" || view.Name == "CurCarrier") && (view.Source == nil || !strings.Contains(strings.ReplaceAll(view.Source.SQL, " ", ""), "root_id=1")) {
				t.Fatalf("authored auxiliary Current filter changed: %+v", view.Source)
			}
		}
		generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		if err != nil {
			t.Fatalf("generation %d: %v", generation, err)
		}
		for _, file := range generated.Result.Files {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			if generation == 2 {
				snapshots[file.Path] = data
			} else if !bytes.Equal(snapshots[file.Path], data) {
				t.Fatalf("regeneration drift: %s", file.Path)
			}
		}
	}
	writeSourceFile(t, root, "generated/regeneration_authored.go", "package generated\ntype RegenerationAuthoredRow struct { Id *int }\n")
	writeSourceFile(t, root, "generated/stock_regenerated_projection_test.go", stockRegeneratedProjectionRuntime)
	cmd := exec.Command("go", "test", "-mod=readonly", "-count=1", "-run", "^TestRegeneratedCurrent", "./generated")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("regenerated helper runtime: %v\n%s", err, out)
	}

}

func TestStockCurrentWildcardEvolutionThroughAuxiliaryParent(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	schema, err := stockCurrentResources.ReadFile("stock_current_resources/expanded-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(schema), ";") {
		if strings.TrimSpace(stmt) != "" {
			if err = db.ExecStatements(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/stockcurrentfixture"}).Write(t, root)
	text, err := stockCurrentResources.ReadFile("stock_current_resources/nested.dql")
	if err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "Probe", Scope: "github.com/viant/datly/stockcurrentfixture/source", Connector: "main", Text: string(text), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	first, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if stockArtifactsContain(t, first, "EvolvedNote") {
		t.Fatal("unexpected future column")
	}
	if err = db.ExecStatements(ctx, "ALTER TABLE children ADD COLUMN evolved_note TEXT"); err != nil {
		t.Fatal(err)
	}
	next, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if !stockArtifactsContain(t, next, "EvolvedNote") || !stockArtifactsContain(t, next, "evolved_note") {
		t.Fatal("wildcard schema evolution absent")
	}
	if source.Text != string(text) {
		t.Fatal("DQL changed")
	}
	stockAssertCompleteAuthority(t, source, root, map[string]bool{"Probe": false, "Carrier": true, "Children": false}, map[string]string{"Carrier": "Probe", "Children": "Carrier"})
	t.Log("wildcard schema changed body/Current generated contracts without DQL/Go edits")
}
func TestStockCurrentPackageDiscoveryRegeneratesSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	schema, err := stockCurrentResources.ReadFile("stock_current_resources/expanded-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(schema), ";") {
		if strings.TrimSpace(stmt) != "" {
			if err = db.ExecStatements(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/stockcurrentfixture"}).Write(t, root)
	text, err := stockCurrentResources.ReadFile("stock_current_resources/absent-physical-current.dql")
	if err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "Probe", Scope: "github.com/viant/datly/stockcurrentfixture/source", Connector: "main", Text: string(text), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	first, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if stockArtifactsContain(t, first, "EvolvedNote") {
		t.Fatal("unexpected future column")
	}
	if err = db.ExecStatements(ctx, "ALTER TABLE children ADD COLUMN evolved_note TEXT"); err != nil {
		t.Fatal(err)
	}
	discoveryText := "#import('generated','github.com/viant/datly/stockcurrentfixture/generated')\n" + source.Text
	writeSourceFile(t, root, "source/probe.dql", discoveryText)
	discovery := &Discovery{BaseDir: root, Include: []string{"github.com/viant/datly/stockcurrentfixture/source"}, SourceFile: "probe.dql", Connector: "main", ColumnRefiner: source.ColumnRefiner}
	for generation := 2; generation <= 3; generation++ {
		project, err := discovery.Compile(ctx)
		if err != nil {
			t.Fatalf("discovery generation %d: %v", generation, err)
		}
		next, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
		if err != nil {
			t.Fatalf("generation %d: %v", generation, err)
		}
		if !stockArtifactsContain(t, next, "EvolvedNote") || !stockArtifactsContain(t, next, "evolved_note") {
			t.Fatal("wildcard schema evolution absent")
		}
		found := false
		for _, view := range next.Result.Plan.Views {
			if view.Type == "CurrentChildrenView" {
				for _, field := range view.Fields {
					if field.Name == "EvolvedNote" {
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatal("automatic child Current retained stale schema")
		}
	}

	if source.Text != string(text) {
		t.Fatal("DQL changed")
	}
	t.Log("wildcard schema changed body/Current generated contracts without DQL/Go edits")
}
func stockArtifactsContain(t *testing.T, g *GeneratedPackage, word string) bool {
	t.Helper()
	for _, f := range g.Result.Files {
		if strings.HasSuffix(f.Path, ".go") {
			b, e := os.ReadFile(f.Path)
			if e != nil {
				t.Fatal(e)
			}
			if strings.Contains(string(b), word) {
				return true
			}
		}
	}
	return false
}

const stockRegeneratedProjectionRuntime = `package generated
import("testing";"reflect";"context";"os";"path/filepath";"github.com/viant/x";"github.com/viant/datly/transcribe";"github.com/viant/datly/typecatalog")
func TestRegeneratedCurrentProjection(t *testing.T){
 method:=reflect.ValueOf(&Input{}).MethodByName("ProjectCurrentChildrenParentKeys")
 if !method.IsValid(){t.Fatal("missing regenerated helper")}
 previous:=reflect.MakeSlice(method.Type().In(0),2,2)
 row:=reflect.New(previous.Type().Elem().Elem())
 for i:=0;i<row.Elem().NumField();i++{field:=row.Elem().Field(i);if field.Kind()==reflect.Ptr && field.Type().Elem().Kind()>=reflect.Int && field.Type().Elem().Kind()<=reflect.Int64 {value:=reflect.New(field.Type().Elem());value.Elem().SetInt(17);field.Set(value)}}
 previous.Index(1).Set(row)
 result:=method.Call([]reflect.Value{previous})
 if !result[1].IsNil() || result[0].Len()!=1 {t.Fatal("parent projection",result)}
 tuple:=result[0].Index(0)
 if tuple.NumField()!=1 || tuple.Type().Field(0).Tag.Get("sqlx")!="carrier_id" {t.Fatal("immediate parent key metadata",tuple.Type())}
 key:=tuple.Field(0);if key.Kind()==reflect.Ptr{key=key.Elem()};if key.Int()!=17{t.Fatal("wrong immediate parent identity",key)}
 if previous.Index(0).IsNil()==false {t.Fatal("Current input mutated")}
}

func TestRegeneratedCurrentLinkedDiscovery(t *testing.T){
 dir,err:=os.Getwd();if err!=nil{t.Fatal(err)}
 registry:=x.NewRegistry()
 generated:=[]reflect.Type{reflect.TypeFor[Input](),reflect.TypeFor[Output](),reflect.TypeFor[Root](),reflect.TypeFor[Carrier](),reflect.TypeFor[PhysicalChild](),reflect.TypeFor[CurrentProbeView](),reflect.TypeFor[CurrentCarrierView](),reflect.TypeFor[CurrentChildrenView]()}
 for _,typ:=range generated{registry.Register(x.NewType(typ))}
 authored:=reflect.TypeFor[RegenerationAuthoredRow]();registry.Register(x.NewType(authored))
 project,err:=(&transcribe.Discovery{BaseDir:filepath.Dir(dir),Include:[]string{"github.com/viant/datly/stockcurrentfixture/source"},SourceFile:"probe.dql",Registry:registry}).Compile(context.Background())
 if err!=nil{t.Fatal(err)}
 compiled:=project.Components[0]
 for _,typ:=range append(generated,authored){
  key:=typ.PkgPath()+"."+typ.Name();resolution,err:=compiled.TypeResolver.ResolveWithProvenance(key);if err!=nil||resolution==nil{t.Fatal("missing source origin",key,err)}
  want:=typecatalog.TypeOriginGenerated;if typ==authored{want=typecatalog.TypeOriginPackage};if resolution.Origin!=want{t.Fatal("runtime link changed source origin",key,resolution.Origin,want)}
  descriptor,err:=compiled.TypeResolver.Descriptor(key);if err!=nil||descriptor==nil||descriptor.Type!=typ{t.Fatal("runtime identity lost",key,err)}
 }
}
`
