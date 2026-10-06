package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

func stockExpandedCurrentFixture(t *testing.T, name string) (*Source, string) {
	t.Helper()
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
	text, err := stockCurrentResources.ReadFile("stock_current_resources/" + name + ".dql")
	if err != nil {
		t.Fatal(err)
	}
	return &Source{Name: "Probe", Scope: "github.com/viant/datly/stockcurrentfixture/source", Connector: "main", Text: string(text), ColumnRefiner: column.New(column.Connections{"main": db.DB})}, root
}

var stockExpandedNegativeStages = map[string]string{
	"authority-absent":          `auxiliary lookup view::Carrier|namespace:Carrier with nested relations requires explicit authored current authority`,
	"authority-query-conflict":  `auxiliary current CurrentCarrier conflicts with authored binding`,
	"authority-header-conflict": `auxiliary current CurrentCarrier conflicts with authored binding`,
	"authority-ambiguous":       `generation current-state input for Carrier is ambiguous`,
}

func TestStockExpandedCurrentObservedAdmissionStages(t *testing.T) {
	for name, want := range stockExpandedNegativeStages {
		t.Run(name, func(t *testing.T) {
			source, root := stockExpandedCurrentFixture(t, name)
			before := source.Text
			_, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("intended native admission stage %q, actual %v", want, err)
			}
			if source.Text != before {
				t.Fatal("source changed")
			}
			stockCurrentNoEmission(t, root)
			t.Logf("ACTUAL_ADMISSION_STAGE=%v; native schema/DQL discovery reached intended role authority; no Go emission", err)
		})
	}
}

// Exact Current mappings for two physical roles reading the same genuine table.
func TestStockExpandedTwoPhysicalCurrentRoles(t *testing.T) {
	source, root := stockExpandedCurrentFixture(t, "two-physical-roles")
	before := source.Text
	generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	snapshots := map[string][]byte{}
	for _, f := range generated.Result.Files {
		b, e := os.ReadFile(f.Path)
		if e != nil {
			t.Fatal(e)
		}
		snapshots[f.Path] = b
	}
	if len(snapshots) == 0 {
		t.Fatal("no native artifacts")
	}
	if _, err = (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root}); err != nil {
		t.Fatal(err)
	}
	for path, want := range snapshots {
		got, e := os.ReadFile(path)
		if e != nil || !reflect.DeepEqual(want, got) {
			t.Fatalf("artifact drift %s %v", path, e)
		}
	}
	if source.Text != before {
		t.Fatal("source changed")
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "stock_expanded_current_metadata_test.go"), []byte(stockExpandedTwoRoleMetadata), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=readonly", "-race", "-count=3", "-v", "-run", "^TestStockTwoPhysicalGeneratedCurrentMetadata$", "./generated")
	command.Dir = root
	out, e := command.CombinedOutput()
	t.Logf("generated native metadata:\n%s", out)
	if e != nil {
		t.Fatalf("generated metadata %v", e)
	}
	t.Logf("TWO_SAME_TABLE_CURRENT_NATIVE_STABLE_ARTIFACTS=%d", len(snapshots))
}

// Desired nested counterparts are retained as failures in the immutable baseline.
func TestStockExpandedNestedDesiredCanonicalGeneration(t *testing.T) {
	for _, name := range []string{"deep-aux", "deep-aux-root", "physical-aux-physical"} {
		t.Run(name, func(t *testing.T) {
			source, root := stockExpandedCurrentFixture(t, name)
			aux := map[string]bool{"Probe": name == "deep-aux-root", "Carrier": true, "Children": false}
			parents := map[string]string{"Carrier": "Probe", "Children": "Chain"}
			if name == "physical-aux-physical" {
				aux["Parent"] = false
				parents["Parent"] = "Probe"
				parents["Carrier"] = "Parent"
				parents["Children"] = "Carrier"
			} else {
				aux["Chain"] = true
				parents["Chain"] = "Carrier"
			}
			stockAssertCompleteAuthority(t, source, root, aux, parents)
		})
	}
}

const stockExpandedTwoRoleMetadata = `package generated
import("testing";"reflect";"github.com/viant/datly/bootstrap";"github.com/viant/datly/tag";"github.com/viant/datly/runtime/handler/writer";sqlxio "github.com/viant/sqlx/io")
func TestStockTwoPhysicalGeneratedCurrentMetadata(t *testing.T){
 holder:=reflect.TypeFor[ProbeComponent]();field,ok:=holder.FieldByName("Contract");if !ok {t.Fatal("missing native Contract")};tags,ok,err:=tag.ParseComponent(field.Tag);if err!=nil||!ok {t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tags,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil {t.Fatal(err)}
 metadata,err:=writer.Compile(component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil {t.Fatal(err)}
 root:=metadata.Root;if root.Name!="Probe"||root.CurrentField<0||len(root.Relations)!=2 {t.Fatal("native graph",root.Name,len(root.Relations))}
 seen:=map[string]int{};for _,rel:=range root.Relations {r:=rel.Child;if r.Name!="Insertions"&&r.Name!="Replacements" {t.Fatal("unexpected role",r.Name)}
  if r.Auxiliary||r.CurrentField<0||len(r.Keys)!=1 {t.Fatal("physical role authority",r.Path)}
  current:=reflect.TypeFor[Input]().Field(r.CurrentField);if current.Name!="Current"+r.Name {t.Fatal("Current mismatch",r.Name,current.Name)}
  parsed,err:=tag.ParseView(current.Tag.Get("view"));if err!=nil||parsed.Table!="children" {t.Fatal("different physical Current table",current.Tag,err)}
  if !sqlxio.ParseTag(r.EntityType.FieldByIndex(r.Keys[0].Index).Tag).PrimaryKey {t.Fatal("PK metadata",r.Path)}
  seen[r.Name]=r.CurrentField;t.Logf("exact physical same-table role=%s Current=%s field=%d nativeView=%s",r.Name,current.Name,r.CurrentField,current.Tag.Get("view"))
 };if len(seen)!=2||seen["Insertions"]==seen["Replacements"] {t.Fatal("role authority conflated",seen)}
}
`
