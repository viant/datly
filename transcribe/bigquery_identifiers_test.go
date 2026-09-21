package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"
)

func TestGeneratedBigQueryIdentifierSQLAndTags(t *testing.T) {
	for _, table := range []string{"`my-project.dataset.parents`", "[project.dataset.parents]", "[project:dataset.parents]"} {
		t.Run(table, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/bqfixture"}).Write(t, root)
			tag := "sqlx:" + strconv.Quote("id,unique,table="+table+",refTable="+table+",refColumn=id")
			text := "#setting($_ = $route('/records','GET'))\nSELECT r.id,CAST(r.id AS int),tag(r.id,'" + tag + "') FROM " + table + " r"
			compiler := NewCompiler()
			compiled, err := compiler.Compile(context.Background(), &Source{Scope: "github.com/viant/datly/bqfixture/generated", Name: "Records", Text: text, Types: typecatalog.NewCatalog()})
			if err != nil {
				t.Fatal(err)
			}
			if compiled.Component.RootView.Source.Table != table || !strings.Contains(compiled.Component.RootView.Source.SQL, table) {
				t.Fatalf("lost executable source: %+v", compiled.Component.RootView.Source)
			}
			input, dir, err := generationInput(root, "generated", compiled)
			if err != nil {
				t.Fatal(err)
			}
			generated, err := compiler.generateInputAt(context.Background(), root, dir, compiled, input)
			if err != nil {
				t.Fatal(err)
			}
			if len(generated.Result.Plan.Views) == 0 {
				t.Fatal("missing generated view")
			}
			smoke := fmt.Sprintf(`package records
import("reflect";"strings";"testing";"github.com/viant/sqlx/io/validator";"github.com/viant/datly/tag";"github.com/viant/bindly/resource")
func TestGeneratedAuthoredConstraintSQL(t *testing.T){
 checks,err:=validator.NewChecks(reflect.TypeOf(%s{}),nil);if err!=nil{t.Fatal(err)}
 if len(checks.Unique)!=1||len(checks.RefKey)!=1{t.Fatalf("missing authored checks: %%+v",checks)}
 table:=%s
 for _,check:=range []*validator.Check{checks.Unique[0],checks.RefKey[0]}{if !strings.Contains(check.SQL,"FROM "+table+" WHERE"){t.Fatalf("lost SQL quoting: %%s",check.SQL)}}
 if checks.RefKey[0].Reference.Table!=table{t.Fatalf("lost receipt text: %%+v",checks.RefKey[0].Reference)}
 output:=reflect.TypeOf(%s{});found:=false
 resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
 for i:=0;i<output.NumField();i++{
  field:=output.Field(i);view,err:=tag.ParseView(field.Tag.Get("view"));if err!=nil{t.Fatal(err)}
  if view==nil||view.Table==""{continue};found=true
  metadata:=tag.ParseSQL(field.Tag.Get("sql"));sql:=metadata.Text
  if metadata.URI!=""{data,err:=resources.ReadFile(metadata.URI);if err!=nil{t.Fatal(err)};sql=string(data)}
  if view.Table!=table||!strings.Contains(sql,table){t.Fatalf("lost generated view source: %%s / %%s",field.Tag,sql)}
 }
 if !found{t.Fatal("missing generated view metadata")}
}
`, generated.Result.Plan.Views[0].Name, strconv.Quote(table), generated.Result.Plan.Output.Type)
			if err := os.WriteFile(filepath.Join(root, dir, "bigquery_test.go"), []byte(smoke), 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "-timeout=60s", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated package: %v\n%s", err, output)
			}
		})
	}
}
