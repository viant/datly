package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Actual native generation, stable repeat and metadata reconstruction from generated tags.
func stockAssertCompleteAuthority(t *testing.T, source *Source, root string, aux map[string]bool, parents map[string]string) {
	t.Helper()
	before := source.Text
	generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := map[string][]byte{}
	for _, f := range generated.Result.Files {
		b, e := os.ReadFile(f.Path)
		if e != nil {
			t.Fatal(e)
		}
		snapshot[f.Path] = b
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
			t.Fatalf("generation drift %s %v", path, e)
		}
	}
	if source.Text != before {
		t.Fatal("source changed")
	}
	auxEntries := []string{}
	for name, value := range aux {
		auxEntries = append(auxEntries, fmt.Sprintf("%q:%v", name, value))
	}
	parentEntries := []string{}
	for name, value := range parents {
		parentEntries = append(parentEntries, fmt.Sprintf("%q:%q", name, value))
	}
	test := strings.ReplaceAll(stockCompleteAuthorityMetadata, "AUX_EXPECTED", strings.Join(auxEntries, ","))
	test = strings.ReplaceAll(test, "PARENTS_EXPECTED", strings.Join(parentEntries, ","))
	if err = os.WriteFile(filepath.Join(root, "generated", "stock_complete_authority_metadata_test.go"), []byte(test), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=readonly", "-race", "-count=3", "-v", "-run", "^TestStockCompleteGeneratedAuthority$", "./generated")
	cmd.Dir = root
	out, e := cmd.CombinedOutput()
	t.Logf("native generated authority:\n%s", out)
	if e != nil {
		t.Fatalf("generated metadata %v", e)
	}
	t.Logf("COMPLETE_CURRENT_NATIVE_STABLE_ARTIFACTS=%d", len(snapshot))
}

const stockCompleteAuthorityMetadata = `package generated
import("testing";"reflect";"github.com/viant/datly/bootstrap";"github.com/viant/datly/tag";"github.com/viant/datly/runtime/handler/writer";sqlxio "github.com/viant/sqlx/io")
func TestStockCompleteGeneratedAuthority(t *testing.T){
 aux:=map[string]bool{AUX_EXPECTED};parents:=map[string]string{PARENTS_EXPECTED}
 holder:=reflect.TypeFor[ProbeComponent]();field,ok:=holder.FieldByName("Contract");if !ok {t.Fatal("missing native Contract")};tags,ok,err:=tag.ParseComponent(field.Tag);if err!=nil||!ok {t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tags,InputType:"Input",OutputType:"Output"};component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil {t.Fatal(err)};m,err:=writer.Compile(component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil {t.Fatal(err)}
 seen:=map[string]bool{};var walk func(*writer.Record,string);walk=func(r *writer.Record,parent string){expected,ok:=aux[r.Name];if !ok||seen[r.Name]||expected!=r.Auxiliary {t.Fatal("native role",r.Name,r.Auxiliary)};seen[r.Name]=true;if parent!=parents[r.Name] {t.Fatal("wrong immediate parent",r.Name,parent,parents[r.Name])}
 if r.CurrentField<0||len(r.Keys)!=1 {t.Fatal("missing exact Current/PK",r.Path)};current:=reflect.TypeFor[Input]().Field(r.CurrentField);if current.Name!="Current"+r.Name {t.Fatal("wrong Current",r.Name,current.Name)};if !sqlxio.ParseTag(r.EntityType.FieldByIndex(r.Keys[0].Index).Tag).PrimaryKey {t.Fatal("PK",r.Path)}
 if r.Auxiliary {if r.Sequence!=nil||r.Selector!="" {t.Fatal("aux physical ownership",r.Path)}} else if r.Sequence==nil||r.Selector=="" {t.Fatal("missing physical ownership",r.Path)}
 t.Logf("role=%s parent=%s Current=%s path=%s auxiliary=%v",r.Name,parent,current.Name,r.Path,r.Auxiliary);for _,rel:=range r.Relations {walk(rel.Child,r.Name)}};walk(m.Root,"");if len(seen)!=len(aux){t.Fatal("missing roles",seen,aux)}
}
`
