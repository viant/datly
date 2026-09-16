package golang

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

func TestMutationScaffoldTypedRolesAndImmutablePlan(t *testing.T) {
	semantic := recursiveSemanticPlan(plan.OperationPost)
	child := semantic.Root.Relations[0].Child
	last := child.Relations[0].Child
	for i, record := range []*plan.RecordPlan{semantic.Root, child, last} {
		record.Entity = &plan.EntityPlan{Hooks: spec.TypeRef{Package: "example.com/scaffoldfixture", Name: []string{"OrderRules", "ItemRules", "DetailRules"}[i]}, HooksScaffold: true}
	}
	child.SelfRelations = []plan.SelfRelationPlan{{FieldPath: plan.FieldPath{"Children"}, Links: []plan.KeyLink{{Parent: child.Keys[0], Child: child.Keys[0]}}}}
	config := Config{Package: "fixture", PackagePath: "example.com/scaffoldfixture", Factory: "NewWriter", InputType: "Input", OutputType: "Output", Records: []RecordType{
		{Identity: semantic.Root.Identity, Path: semantic.Root.InputPath, Value: "[]*Order"},
		{Identity: child.Identity, Path: child.InputPath, Value: "[]*Node"},
		{Identity: last.Identity, Path: last.InputPath, Value: "[]*Node"},
	}}
	before, _ := json.Marshal(semantic)
	asset, err := ScaffoldMutationHooks(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(semantic)
	if !bytes.Equal(before, after) {
		t.Fatal("semantic plan mutated")
	}
	if len(asset.Bindings) != 3 {
		t.Fatalf("roles=%+v", asset.Bindings)
	}
	var source bytes.Buffer
	if err = format.Node(&source, token.NewFileSet(), asset.File); err != nil {
		t.Fatal(err)
	}
	again, err := ScaffoldMutationHooks(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	var repeated bytes.Buffer
	_ = format.Node(&repeated, token.NewFileSet(), again.File)
	if source.String() != repeated.String() {
		t.Fatal("role naming or source is not deterministic")
	}
	names := make([]string, 3)
	for i, binding := range asset.Bindings {
		ref, err := (xshape.Resolver{}).Reference(binding.Hook)
		if err != nil {
			t.Fatal(err)
		}
		names[i] = ref.BaseName
	}
	if names[0] != "OrderRules" || names[1] != "ItemRules" || names[2] != "DetailRules" {
		t.Fatalf("lifecycle names = %v", names)
	}
	if names[1] == names[2] || asset.Bindings[1].Entity != asset.Bindings[2].Entity || asset.Bindings[1].Parent == asset.Bindings[2].Parent {
		t.Fatal("role/parent authority collapsed")
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: config.PackagePath}).Write(t, root)
	if err = os.WriteFile(filepath.Join(root, "hooks.go"), source.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	fixture := fmt.Sprintf(`package fixture
import("context";"testing";h "github.com/viant/xdatly/handler";m "github.com/viant/xdatly/handler/mutation")
type Order struct{Items []*Node};type Node struct{Details,Children []*Node};type Input struct{Orders []*Order};type Output struct{Data []*Order}
var _ h.EntityHooks[Order,h.NoParent,Output]=(*%s)(nil)
var _ h.EntityHooks[Node,Order,Output]=(*%s)(nil)
var _ h.EntityHooks[Node,Node,Output]=(*%s)(nil)
var _ h.AfterSequenceHook[Node,Order,Output]=(*%s)(nil)
var _ h.AfterQueueHook[Node,Order,Output]=(*%s)(nil)
var _ m.Finalizer[Input,Output]=(*%s)(nil)
func TestTypedState(t *testing.T){ctx:=context.Background();output:=&Output{};state:=h.LifecycleContext[Node,Order,Output]{EntityState:h.EntityState[Node,Order]{Parent:&Order{},SelfParent:&Node{}},Output:output};hooks:=&%s{};if err:=hooks.Init(ctx,&Node{},state);err!=nil{t.Fatal(err)};if err:=hooks.Validate(ctx,&Node{},state);err!=nil{t.Fatal(err)}}
`, names[0], names[1], names[2], names[1], names[1], names[0], names[1])
	if err = os.WriteFile(filepath.Join(root, "hooks_test.go"), []byte(fixture), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("typed scaffold: %v\n%s\n%s", err, out, source.String())
	}
	semantic.Root.Entity.HooksScaffold = false
	semantic.Root.Entity.Hooks = spec.TypeRef{Package: "example.com/authored", Name: "Hooks"}
	preserved, err := ScaffoldMutationHooks(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(preserved.Bindings) != 2 || preserved.Plan.Root.Entity.Hooks != semantic.Root.Entity.Hooks {
		t.Fatal("authored hook overwritten")
	}
	for _, record := range []*plan.RecordPlan{child, last} {
		record.Entity.HooksScaffold = false
		record.Entity.Hooks = spec.TypeRef{Package: "example.com/authored", Name: "Hooks"}
	}
	preserved, err = ScaffoldMutationHooks(semantic, config)
	if err != nil || preserved.File != nil || len(preserved.Bindings) != 0 {
		t.Fatal("fully authored graph produced a scaffold", err)
	}
	child.Entity = nil
	if _, err = ScaffoldMutationHooks(semantic, config); err == nil {
		t.Fatal("missing canonical entity metadata accepted")
	}
	if strings.Contains(source.String(), "func (input *") {
		t.Fatal("direct lifecycle leaked into mutation scaffold")
	}
}

func TestMutationScaffoldHookless(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{}
	config := Config{Package: "fixture", PackagePath: "example.com/fixture", Factory: "NewWriter", InputType: "Input", OutputType: "Output", Records: []RecordType{{Identity: semantic.Root.Identity, Path: semantic.Root.InputPath, Value: "[]*Order"}}}
	result, err := ScaffoldMutationHooks(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.File != nil || len(result.Bindings) != 0 || !result.Plan.Root.Entity.Hooks.IsZero() {
		t.Fatal("implicit lifecycle created")
	}
}
