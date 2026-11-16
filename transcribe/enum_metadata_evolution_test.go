package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestEnumMetadataSelectorCubeEvolutionRegression(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	text := `#package('example.com/generated/rows')
#setting($_ = $route('/rows','GET'))
#setting($_ = $input_type('example.com/generated/rows.RowsInput'))
#setting($_ = $output_type('example.com/generated/rows.RowsOutput'))
#setting($_ = $cube('', '', '', '', '', '', '', true))
#define($_ = $Mode<string>(query/mode).Optional())
#define($_ = $Limit<int>(query/limit).QuerySelector('r'))
#define($_ = $Rows<[]*Row>(output/view))
SELECT r.*,type(r,'Row'),grouping_enabled(r) FROM (SELECT category,SUM(amount) AS amount FROM records GROUP BY category) r`
	resolver := func(_ context.Context, r column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
		return &column.PreparedColumnResult{Columns: []*spec.Column{{Name: "category", Source: "category", Type: spec.TypeRef{Name: "string"}}, {Name: "amount", Source: "amount", Type: spec.TypeRef{Name: "int"}}}}, nil
	}
	file := filepath.Join(root, "source", "Rows.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
	require.NoError(t, os.WriteFile(file, []byte(text), 0600))
	run := func() error {
		d := &Discovery{BaseDir: root, PreparedColumns: resolver}
		_, err := d.TranscribeBatch(ctx, BatchRequest{Generator: Generator{Operation: "get"}, Destination: root, Rules: []BatchRule{{Source: file}}})
		return err
	}
	require.NoError(t, run(), "initial native generation")
	child := `package rows
import("context";"os";"path/filepath";"reflect";"strings";"testing";"crypto/sha256";"fmt";"io/fs";"time";"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";"github.com/viant/datly/transcribe";"github.com/viant/datly/transcribe/column")
func TestNativeEvolution(t *testing.T){root,err:=filepath.Abs("..");if err!=nil{t.Fatal(err)};found,err:=bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[RowsInput]().PkgPath()},nil);if err!=nil{t.Fatal(err)}
resolver:=func(_ context.Context,r column.PreparedColumnRequest)(*column.PreparedColumnResult,error){return &column.PreparedColumnResult{Columns:[]*spec.Column{{Name:"category",Source:"category",Type:spec.TypeRef{Name:"string"}},{Name:"amount",Source:"amount",Type:spec.TypeRef{Name:"int"}}}},nil}
file:=filepath.Join(root,"source/Rows.dql");invocation:=0;run:=func()error{invocation++;start:=time.Now();defer func(){t.Logf("native transcription invocation=%d elapsed=%s",invocation,time.Since(start))}();d:=&transcribe.Discovery{BaseDir:root,Types:found.Types,PreparedColumns:resolver};_,e:=d.TranscribeBatch(context.Background(),transcribe.BatchRequest{Generator:transcribe.Generator{Operation:"get"},Destination:root,Rules:[]transcribe.BatchRule{{Source:file}}});return e}
if os.Getenv("ENUM_FRESH_RELOAD")=="1" {has,ok:=reflect.TypeFor[RowsInput]().FieldByName("Has");if !ok||has.Type.Kind()!=reflect.Pointer||has.Type.Elem().Name()!="RowsInputHas"{t.Fatal("public named presence ABI changed")};if _,ok:=reflect.TypeFor[RowsInput]().FieldByName("Limit");ok{t.Fatal("selector leaked into public input")};if _,ok:=reflect.TypeFor[*RowsInput]().MethodByName("SetMode");!ok{t.Fatal("public setter missing")};filters,ok:=reflect.TypeFor[RowsCubeInput]().FieldByName("Filters");if !ok{t.Fatal("cube filters missing")};mode,ok:=filters.Type.FieldByName("Mode");if !ok||mode.Type!=reflect.TypeFor[*string](){t.Fatal("cube Mode filter wire/presence type changed")};marker,ok:=filters.Type.FieldByName("Has");if !ok||marker.Type.Kind()!=reflect.Pointer{t.Fatal("cube filter presence missing")};f,_:=reflect.TypeFor[RowsInput]().FieldByName("Mode");if f.Tag.Get("enumAccept")!="scalar"||f.Tag.Get("enumStyle")!="literal"||!strings.Contains(string(f.Tag),"choice:\"day\" choice:\"hour\""){t.Fatal("fresh enum tags missing",f.Tag)};snapshot:=func()string{var all strings.Builder;err:=filepath.WalkDir(filepath.Join(root,"rows"),func(p string,d fs.DirEntry,e error)error{if e!=nil{return e};if !d.IsDir(){b,e:=os.ReadFile(p);if e!=nil{return e};fmt.Fprintf(&all,"%s:%x\n",p,sha256.Sum256(b))};return nil});if err!=nil{t.Fatal(err)};return all.String()};if err=run();err!=nil{t.Fatal("fresh reload:",err)};before:=snapshot();for i:=0;i<2;i++{if err=run();err!=nil{t.Fatal("parity pass:",err)};if after:=snapshot();after!=before{t.Fatal("full generated tree changed on repeat")}};return};if err=run();err!=nil{t.Fatal("unchanged baseline:",err)};body,err:=os.ReadFile(file);if err!=nil{t.Fatal(err)};edited:=strings.Replace(string(body),"(query/mode).Optional()","(query/mode).Optional().WithTag('choice:\"day\" choice:\"hour\" enumAccept:\"scalar\" enumStyle:\"literal\"')",1);if err=os.WriteFile(file,[]byte(edited),0600);err!=nil{t.Fatal(err)}
err=run();if err!=nil{t.Fatalf("enum-only authoring: %v",err)};t.Log("enum-only source transition succeeded")
}`
	require.NoError(t, os.WriteFile(filepath.Join(root, "rows/evolution_test.go"), []byte(child), 0600))
	cmd := exec.Command("go", "test", "-v", "./rows", "-run", "TestNativeEvolution", "-count=1")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	t.Log(string(out))
	require.NoError(t, err)
	cmd = exec.Command("go", "test", "-v", "./rows", "-run", "TestNativeEvolution", "-count=1")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "ENUM_FRESH_RELOAD=1")
	out, err = cmd.CombinedOutput()
	t.Log(string(out))
	require.NoError(t, err)
}
