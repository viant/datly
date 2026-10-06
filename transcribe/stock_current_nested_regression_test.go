package transcribe

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
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

//go:embed stock_current_resources/*
var stockCurrentResources embed.FS

func stockCurrentFixture(t *testing.T, name string) (*Source, string) {
	t.Helper()
	ctx := context.Background()
	db := sqlite.New(t)
	schema, err := stockCurrentResources.ReadFile("stock_current_resources/schema.sql")
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

func stockCurrentNoEmission(t *testing.T, root string) {
	t.Helper()
	for _, dir := range []string{"generated", "source"} {
		files, _ := filepath.Glob(filepath.Join(root, dir, "*.go"))
		if len(files) > 0 {
			t.Fatalf("negative emitted Go: %v", files)
		}
	}
}

// The immutable baseline packet retains the failing counterpart.
func TestStockCurrentAuxiliaryNestedDesiredCanonicalGeneration(t *testing.T) {
	source, root := stockCurrentFixture(t, "nested")
	stockAssertCompleteAuthority(t, source, root, map[string]bool{"Probe": false, "Carrier": true, "Children": false}, map[string]string{"Carrier": "Probe", "Children": "Carrier"})
}

func TestStockCurrentRootSiblingControl(t *testing.T) {
	source, root := stockCurrentFixture(t, "sibling")
	before := source.Text
	generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := map[string][]byte{}
	hashes := map[string]string{}
	for _, file := range generated.Result.Files {
		b, e := os.ReadFile(file.Path)
		if e != nil {
			t.Fatal(e)
		}
		snapshot[file.Path] = b
		hashes[filepath.Base(file.Path)] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	if len(snapshot) == 0 {
		t.Fatal("no native artifacts")
	}
	if _, err = (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root}); err != nil {
		t.Fatal(err)
	}
	for path, want := range snapshot {
		got, e := os.ReadFile(path)
		if e != nil || !reflect.DeepEqual(want, got) {
			t.Fatalf("generation drift %s: %v", path, e)
		}
	}
	if source.Text != before {
		t.Fatal("source changed")
	}
	// Authored metadata assertion only; generated sources remain untouched.
	if err = os.WriteFile(filepath.Join(root, "generated", "stock_current_metadata_test.go"), []byte(stockCurrentMetadataTest), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=readonly", "-race", "-count=3", "-v", "-run", "^TestStockGeneratedCurrentMetadata$", "./generated")
	cmd.Dir = root
	out, e := cmd.CombinedOutput()
	t.Logf("generated metadata output:\n%s", out)
	if e != nil {
		t.Fatalf("generated metadata: %v", e)
	}
	t.Logf("STOCK_SIBLING_GENERATED_STABLE_ARTIFACTS=%d SHA256=%v", len(snapshot), hashes)
}

const stockCurrentMetadataTest = `package generated
import("testing";"reflect";"github.com/viant/datly/bootstrap";"github.com/viant/datly/tag";"github.com/viant/datly/runtime/handler/writer";sqlxio "github.com/viant/sqlx/io")
func TestStockGeneratedCurrentMetadata(t *testing.T) {
 holder:=reflect.TypeFor[ProbeComponent]();field,ok:=holder.FieldByName("Contract");if !ok {t.Fatal("missing generated Contract")};tags,ok,err:=tag.ParseComponent(field.Tag);if err!=nil||!ok {t.Fatal("generated tag",err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tags,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil {t.Fatal(err)}
 m,err:=writer.Compile(component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil {t.Fatal(err)}
 if m.Root.Name!="Probe"||m.Root.Auxiliary||len(m.Root.Relations)!=2 {t.Fatal("wrong genuine root/sibling graph",m.Root.Name,m.Root.Auxiliary,len(m.Root.Relations))}
 seen:=map[string]bool{};var walk func(*writer.Record);walk=func(r *writer.Record){
  if r.CurrentField<0 {t.Fatal("no exact Current",r.Path)};name:=reflect.TypeFor[Input]().Field(r.CurrentField).Name;if name!="Current"+r.Name {t.Fatal("wrong Current",r.Path,name)}
  if len(r.Keys)!=1 {t.Fatal("missing schema PK",r.Path)};key:=r.EntityType.FieldByIndex(r.Keys[0].Index);kt:=sqlxio.ParseTag(key.Tag);if !kt.PrimaryKey {t.Fatal("untruthful PK",r.Path,key.Tag)};t.Logf("native role=%s Current=%s auxiliary=%v PK=%v autoincrement=%v",r.Name,name,r.Auxiliary,kt.PrimaryKey,kt.Autoincrement)
  if r.Name=="Carrier" {if !r.Auxiliary||r.Sequence!=nil||r.Selector!="" {t.Fatal("auxiliary has physical ownership",r.Path)}} else {if r.Auxiliary||r.Sequence==nil||r.Selector=="" {t.Fatal("physical ownership absent",r.Path)}}
  seen[r.Name]=true;for _,rel:=range r.Relations {walk(rel.Child)}
 };walk(m.Root);if len(seen)!=3||!seen["Carrier"]||!seen["Children"] {t.Fatal("roles",seen)}
}
`
