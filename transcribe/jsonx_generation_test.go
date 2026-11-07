package transcribe

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
)

func TestJSONXSQLAuthoredTagStableGeneration(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE flags(id INTEGER PRIMARY KEY,flag INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Flags", Scope: "example.com/jsonx", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#setting($_ = $route('/flags','PATCH'))
#setting($_ = $case_format('lc'))
#define($_ = $Flags<[]*FlagsView>(body/data))
#define($_ = $CurrentFlags<?>(view/CurrentFlags).Cardinality('Many') /* SELECT id,flag FROM flags WHERE id IN (#foreach($row in $Flags)$row.Id#if($foreach.HasNext),#end#end) */)
#define($_ = $Data<[]*FlagsView>(output/body))
SELECT f.*, CAST(f.flag AS '*int'), tag(f.flag,'jsonx:"boolToInt,null=omit"') FROM flags f`}
	request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Current: "CurrentFlags"}}}
	var first map[string]string
	for i := 0; i < 2; i++ {
		if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{}
		found := false
		err := filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files[relative] = string(data)
			if strings.Contains(string(data), `jsonx:"boolToInt,null=omit"`) {
				found = true
			}
			if strings.Contains(string(data), "func (") && (strings.Contains(string(data), "MarshalJSON(") || strings.Contains(string(data), "UnmarshalJSON(")) {
				t.Errorf("generated custom JSON method in %s", relative)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatal("authored input policy lost during native generation")
		}
		if i == 0 {
			first = files
		} else if !reflect.DeepEqual(first, files) {
			t.Fatal("identical transcription changed generated artifacts")
		}
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "jsonx_runtime_test.go"), []byte(`package flags
import (
 "context"
 "reflect"
 "testing"
 "github.com/viant/bindly/provider/body"
)
func TestGeneratedJSONXBody(t *testing.T) {
 for _, tc := range []struct{ raw string; want int; present bool }{
  {"true",1,true},{"false",0,true},{"2",2,true},{"-1",-1,true},{"null",0,false},
 } {
  source, err := body.New([]byte("{\"Flag\":"+tc.raw+"}"), "application/json", nil)
  if err != nil { t.Fatal(err) }
  value, _, err := source.Value(context.Background(), reflect.TypeFor[*FlagsView](), "")
  if err != nil { t.Fatal(err) }
  row := value.(*FlagsView)
  if (row.Flag != nil) != tc.present || row.Flag != nil && *row.Flag != tc.want || row.Has == nil || row.Has.Flag != tc.present { t.Fatalf("input=%s row=%+v",tc.raw,row) }
 }
}
`), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "go", "test", "-mod=mod", "-race", "-count=1", "./generated")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated runtime: %v\n%s", err, output)
	}
}
