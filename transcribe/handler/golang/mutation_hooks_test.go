package golang

import (
	"bytes"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	model "github.com/viant/datly/transcribe/testdata/mutationhooks"
)

func TestMutationHookSupportScalarInstanceWithoutBinder(t *testing.T) {
	pkg := reflect.TypeOf(model.ScalarHooks(0)).PkgPath()
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{Hooks: spec.TypeRef{Package: pkg, Name: "ScalarHooks"}}
	asset, err := MutationHookSupport(semantic, Config{Package: "generated", PackagePath: "github.com/viant/datly/hookscalar", Factory: "NewScalar", InputType: "model.Input", OutputType: "model.Output", Imports: []spec.ImportSpec{{Alias: "model", Package: pkg}}, Records: []RecordType{{Identity: semantic.Root.Identity, Path: semantic.Root.InputPath, Value: "[]*model.Row"}}})
	if err != nil {
		t.Fatal(err)
	}
	var source bytes.Buffer
	if err = format.Node(&source, token.NewFileSet(), asset.File); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(source.String(), ".Bind(") {
		t.Fatalf("no-DI scalar hook attempted field binding: %s", source.String())
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/hookscalar"}).Write(t, root)
	if err = os.WriteFile(filepath.Join(root, "hooks.go"), source.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	testSource := `package generated
import("context";"testing";"strings";model "` + pkg + `")
func TestScalar(t *testing.T){
 ctx:=context.Background();hooks:=&_newScalarMutationHooks{}
 frames:=&_newScalarMutationFrames{Role0:[]*_newScalarMutationHooksFrame0{{Entity:&model.Row{}},{Entity:&model.Row{}}}}
 if err:=hooks.Init(ctx,frames);err==nil{t.Fatal("Init before Prepare accepted")}
 if err:=hooks.Prepare(ctx,nil);err!=nil{t.Fatal(err)}
 if err:=hooks.Prepare(ctx,nil);err==nil{t.Fatal("Prepare repeated")}
 if err:=hooks.Init(ctx,frames);err!=nil{t.Fatal(err)}
 if err:=hooks.Init(ctx,frames);err==nil{t.Fatal("Init repeated")}
 if err:=hooks.Validate(ctx,frames);err!=nil{t.Fatal(err)}
 if err:=hooks.Validate(ctx,frames);err==nil{t.Fatal("Validate repeated")}
 if err:=hooks.AfterSequence(ctx,frames);err!=nil{t.Fatal(err)}
 if err:=hooks.AfterQueue(ctx,frames);err!=nil{t.Fatal(err)}
 for _,bad:=range []*_newScalarMutationFrames{{Role0:[]*_newScalarMutationHooksFrame0{nil}},{Role0:[]*_newScalarMutationHooksFrame0{{}}}}{
  invalid:=&_newScalarMutationHooks{};if err:=invalid.Prepare(ctx,nil);err!=nil{t.Fatal(err)}
  if err:=invalid.Init(ctx,bad);err==nil||!strings.Contains(err.Error(),"requires a non-nil frame and entity"){t.Fatalf("bad Init frame=%v",err)}
  invalid=&_newScalarMutationHooks{};if err:=invalid.Prepare(ctx,nil);err!=nil{t.Fatal(err)};if err:=invalid.Init(ctx,frames);err!=nil{t.Fatal(err)}
  if err:=invalid.Validate(ctx,bad);err==nil||!strings.Contains(err.Error(),"requires a non-nil frame and entity"){t.Fatalf("bad Validate frame=%v",err)}
 }
}
`
	if err = os.WriteFile(filepath.Join(root, "hooks_test.go"), []byte(testSource), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-race", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated scalar hooks: %v\n%s\n%s", err, output, source.String())
	}
}

func TestMutationHookSupportRequiresPackageAuthority(t *testing.T) {
	if _, err := MutationHookSupport(rootSemanticPlan(plan.OperationPost, false), Config{}); err == nil {
		t.Fatal("missing canonical package accepted")
	}
}
