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

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

const fkResultAliasSchema = `CREATE TABLE parent(ID INTEGER PRIMARY KEY NOT NULL);
CREATE TABLE child(ID INTEGER PRIMARY KEY NOT NULL, CHANNEL_GROUP_ID INTEGER NOT NULL,
FREQUENCY_CAP_TYPE_ID INTEGER, FOREIGN KEY(CHANNEL_GROUP_ID) REFERENCES parent(ID),
FOREIGN KEY(FREQUENCY_CAP_TYPE_ID) REFERENCES parent(ID));
INSERT INTO parent VALUES(7),(8),(9);
INSERT INTO child VALUES(21,7,9),(22,8,NULL);`

// Compile and execute the generated Go row itself. The SQL below comes from
// the canonical compiler's vendor source; outer configuration names are never
// introduced into executable SQL to make the row mapping pass.
func TestGeneratedForeignKeyResultAliases(t *testing.T) {
	for _, tc := range []struct {
		name, projection, inner, groupField, frequencyField, groupMapping, frequencyMapping string
		dml                                                                                 bool
	}{
		{"inner aliases", "record.*", "SELECT c.ID,c.CHANNEL_GROUP_ID AS CHANNEL_GROUP,c.FREQUENCY_CAP_TYPE_ID AS FREQUENCY_CAP_TYPE FROM child c", "ChannelGroup", "FrequencyCapType", "CHANNEL_GROUP_ID|CHANNEL_GROUP", "FREQUENCY_CAP_TYPE_ID|FREQUENCY_CAP_TYPE", true},
		{"outer names", "record.ID,record.CHANNEL_GROUP_ID AS RENAMED_GROUP,record.FREQUENCY_CAP_TYPE_ID AS RENAMED_FREQUENCY", "SELECT c.ID,c.CHANNEL_GROUP_ID,c.FREQUENCY_CAP_TYPE_ID FROM child c", "RenamedGroup", "RenamedFrequency", "CHANNEL_GROUP_ID", "FREQUENCY_CAP_TYPE_ID", true},
		{"inner alias plus outer names", "record.ID,record.CHANNEL_GROUP AS RENAMED_GROUP,record.FREQUENCY_CAP_TYPE AS RENAMED_FREQUENCY", "SELECT c.ID,c.CHANNEL_GROUP_ID AS CHANNEL_GROUP,c.FREQUENCY_CAP_TYPE_ID AS FREQUENCY_CAP_TYPE FROM child c", "RenamedGroup", "RenamedFrequency", "CHANNEL_GROUP", "FREQUENCY_CAP_TYPE", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			require.NoError(t, db.ExecStatements(ctx, strings.Split(fkResultAliasSchema, ";")...))
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/fkresultfixture"}).Write(t, root)
			source := &Source{Name: "Children", Types: typecatalog.NewCatalog(), Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: "#package('github.com/viant/datly/fkresultfixture/records')\n#setting($_ = $route('/children','GET'))\nSELECT " + tc.projection + " FROM (" + tc.inner + ") record"}
			compiled, err := NewCompiler().Compile(ctx, source)
			require.NoError(t, err)
			generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
			require.NoError(t, err)
			plan := generated.Result.Plan
			require.Len(t, plan.Views, 1)
			row := plan.Views[0]
			found := map[string]string{}
			for _, f := range row.Fields {
				found[f.Name] = reflect.StructTag(f.Tag).Get("sqlx")
			}
			for field, mapping := range map[string]string{tc.groupField: tc.groupMapping, tc.frequencyField: tc.frequencyMapping} {
				require.Equal(t, mapping, strings.Split(found[field], ",")[0], field)
				require.Contains(t, found[field], "refTable=parent", field)
				require.Contains(t, found[field], "refColumn=ID", field)
			}
			require.Contains(t, found[tc.groupField], "required=true")
			require.NotContains(t, found[tc.frequencyField], "required=true")
			// Regeneration must be byte stable before the runtime test is added.
			before := primaryPackageSnapshot(t, root)
			_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
			require.NoError(t, err)
			require.Equal(t, before, primaryPackageSnapshot(t, root))
			vendorSQL := compiled.Component.RootView.Source.SQL
			require.NotContains(t, vendorSQL, "RENAMED_GROUP")
			require.NotContains(t, vendorSQL, "RENAMED_FREQUENCY")
			testSource := fmt.Sprintf(fkGeneratedRuntime, row.Name, fkResultAliasSchema, vendorSQL, tc.groupField, tc.frequencyField, tc.dml)
			require.NoError(t, os.WriteFile(filepath.Join(root, "records", "fk_runtime_test.go"), []byte(testSource), 0600))
			cmd := exec.Command("go", "test", "-race", "-count=1", "-mod=readonly", "./records")
			cmd.Dir = root
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "GOFLAGS=") {
					cmd.Env = append(cmd.Env, value)
				}
			}
			cmd.Env = append(cmd.Env, "GOFLAGS=-mod=readonly")
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
			t.Logf("actual generated Go runtime and physical DML check: %s", out)
		})
	}
}

const fkGeneratedRuntime = `package records
import (
 "context"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/datly/internal/testharness"
 sqlread "github.com/viant/sqlx/io/read"
 sqlinsert "github.com/viant/sqlx/io/insert"
)
func TestActualGeneratedFKRow(t *testing.T) {
 ctx:=context.Background();h:=testharness.NewSQLiteHarness(t)
 if err:=h.ExecStatements(ctx,strings.Split(%[2]q,";")...);err!=nil{t.Fatal(err)}
 rowType:=reflect.TypeFor[%[1]s]();group,frequency:=%[4]q,%[5]q
 r,err:=sqlread.New(ctx,h.DB,%[3]q,func()any{return reflect.New(rowType).Interface()});if err!=nil{t.Fatal(err)}
 count:=0
 err=r.QueryAll(ctx,func(value any)error{
  row:=reflect.ValueOf(value).Elem();id:=row.FieldByName("Id");if id.Kind()==reflect.Pointer{id=id.Elem()}
  g:=row.FieldByName(group);if g.Kind()==reflect.Pointer{if g.IsNil(){t.Fatal("required group is nil")};g=g.Elem()}
  f:=row.FieldByName(frequency)
  if id.Int()==21 {if g.Int()!=7||f.Kind()!=reflect.Pointer||f.IsNil()||f.Elem().Int()!=9{t.Fatalf("nonzero/non-null FK mapping lost: %%v",row.Interface())}} else if id.Int()==22 {if g.Int()!=8||!f.IsNil(){t.Fatalf("nullable FK mapping lost: %%v",row.Interface())}} else {t.Fatalf("unexpected ID: %%v",row.Interface())}
  count++;return nil
 });if r.Stmt()!=nil{if e:=r.Stmt().Close();e!=nil{t.Fatal(e)}};if err!=nil{t.Fatal(err)};if count!=2{t.Fatalf("count=%%d",count)}
 if !%[6]t{return}
 row:=reflect.New(rowType);setInt:=func(name string,n int64){f:=row.Elem().FieldByName(name);if f.Kind()==reflect.Pointer{f.Set(reflect.New(f.Type().Elem()));f=f.Elem()};f.SetInt(n)}
 setInt("Id",33);setInt(group,7);setInt(frequency,9)
 writer,err:=sqlinsert.New(ctx,h.DB,"child");if err!=nil{t.Fatal(err)}
 affected,_,err:=writer.Exec(ctx,row.Interface());if err!=nil{t.Fatalf("writer must use physical FK columns: %%v",err)};if affected!=1{t.Fatalf("affected=%%d",affected)}
 var g,f int;if err=h.DB.QueryRowContext(ctx,"SELECT CHANNEL_GROUP_ID,FREQUENCY_CAP_TYPE_ID FROM child WHERE ID=33").Scan(&g,&f);err!=nil{t.Fatal(err)};if g!=7||f!=9{t.Fatalf("physical DML values: %%d,%%d",g,f)}
}
`
