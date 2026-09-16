package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	xshape "github.com/viant/x/shape"
)

const mutationHookFixture = `package orders
import("context";h "github.com/viant/xdatly/handler")
type Row struct{}
type Output struct{}
type Hooks struct{}
func(*Hooks) Init(context.Context,*Row,h.LifecycleContext[Row,h.NoParent,Output])error{return nil}
func(*Hooks) Validate(context.Context,*Row,h.LifecycleContext[Row,h.NoParent,Output])error{return nil}
func(*Hooks) AfterQueue(context.Context,*Row,h.LifecycleContext[Row,h.NoParent,Output])error{return nil}
`

func testMutationScaffoldAsset(t *testing.T) *HookScaffoldAsset {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", mutationHookFixture, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	asset := &HookScaffoldAsset{Destination: "hooks.go", File: file, PackagePath: "example.com/fixture", EntityHooks: []HookScaffoldContract{{Type: "example.com/fixture.Hooks", Identity: "view:Rows", Path: []string{"Rows"}, Required: []string{"Init", "Validate"}}}}
	// An independently specified valid fixture provides native evidence at this
	// artifact boundary. SDK compilation is tested at root orchestration.
	types, err := asset.ResolveEntityHookTypes("")
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := types.Descriptor(asset.EntityHooks[0].Type)
	if err != nil {
		t.Fatal(err)
	}
	methods, err := xshape.New(descriptor, types.Descriptor).Methods(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = asset.RetainEntityHookEvidence(map[string][]xshape.Method{asset.EntityHooks[0].Type: methods}); err != nil {
		t.Fatal(err)
	}
	return asset
}

func TestMutationScaffoldNativeSourceContracts(t *testing.T) {
	const source = mutationHookFixture
	for _, tc := range []struct {
		name, actual, want string
	}{
		{name: "unchanged", actual: source},
		{name: "import alias and custom body", actual: strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(source, `h "github.com`, `sdk "github.com`), "h.", "sdk."), "return nil", `panic("application body")`)},
		{name: "optional method removed", actual: source[:strings.Index(source, "func(*Hooks) AfterQueue")]},
		{name: "value receiver", actual: strings.ReplaceAll(source, "(*Hooks)", "(Hooks)")},
		{name: "wrong result", actual: strings.Replace(source, ")error", ")string", 1), want: "incompatible signature"},
		{name: "variadic state", actual: strings.Replace(source, ",h.LifecycleContext", ",...h.LifecycleContext", 1), want: "incompatible signature"},
		{name: "required method removed", actual: strings.Replace(source, " Init(", " CustomInit(", 1), want: "requires Init"},
		{name: "parent contract changed", actual: strings.Replace(source, "h.NoParent", "Row", 1), want: "incompatible signature"},
		{name: "wrong package", actual: strings.Replace(source, "package orders", "package other", 1), want: "differs from generated package"},
		{name: "new role absent from existing file", actual: "package orders\ntype Authored struct{}", want: "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asset := testMutationScaffoldAsset(t)
			var err error
			root := t.TempDir()
			if err = os.WriteFile(filepath.Join(root, "hooks.go"), []byte(tc.actual), 0644); err != nil {
				t.Fatal(err)
			}
			// Entity definitions may be generated later. Native signature loading
			// must retain their canonical identities without manufacturing types.
			err = asset.validateMutationMethods(root)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("validation=%v, want %q", err, tc.want)
			}
			actual, err := os.ReadFile(filepath.Join(root, "hooks.go"))
			if err != nil || string(actual) != tc.actual {
				t.Fatal("signature inspection changed user source", err)
			}
			// The same controls apply to a modified proposal, not only to the
			// existing create-once source at persistence time.
			asset.File, err = parser.ParseFile(token.NewFileSet(), "hooks.go", tc.actual, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			want := tc.want
			if tc.name == "wrong package" {
				want = "package or role contract changed"
			}
			err = asset.validateMutationMethods("")
			if want == "" && err != nil || want != "" && (err == nil || !strings.Contains(err.Error(), want)) {
				t.Fatalf("proposal validation=%v, want %q", err, want)
			}
			if want == "" {
				input := Input{Component: customHandlerComponent(), TargetPackage: asset.PackagePath, MutationHandler: mutationAsset(t, mutationDefinitionFixture), HookScaffold: asset}
				if _, err = New(input).Generate(filepath.Join(t.TempDir(), "compatible")); err != nil {
					t.Fatalf("compatible proposal rejected at public generation boundary: %v", err)
				}
			}
		})
	}
}

func TestMutationScaffoldRevalidatesFreshAssetsAndPlans(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*HookScaffoldAsset)
		want   string
	}{
		{"missing evidence", func(a *HookScaffoldAsset) { a.evidence = nil }, "compiler-approved"},
		{"malformed Init", func(a *HookScaffoldAsset) {
			for _, decl := range a.File.Decls {
				if method, ok := decl.(*ast.FuncDecl); ok && method.Name.Name == "Init" {
					method.Type.Params.List = method.Type.Params.List[:1]
				}
			}
		}, "incompatible signature"},
		{"changed role", func(a *HookScaffoldAsset) { a.EntityHooks[0].Path[0] = "Other" }, "role contract changed"},
		{"removed mandatory requirement", func(a *HookScaffoldAsset) { a.EntityHooks[0].Required = a.EntityHooks[0].Required[:1] }, "role contract changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asset := testMutationScaffoldAsset(t)
			input := Input{Component: customHandlerComponent(), TargetPackage: asset.PackagePath, MutationHandler: mutationAsset(t, mutationDefinitionFixture), HookScaffold: asset}
			plan, err := New(input).Plan()
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			existing := filepath.Join(root, "existing")
			if _, err = EmitScaffold(existing, plan); err != nil {
				t.Fatal(err)
			}
			before := map[string][]byte{}
			entries, err := os.ReadDir(existing)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					before[entry.Name()], err = os.ReadFile(filepath.Join(existing, entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(before[".datly-gen.json"]) == 0 {
				t.Fatal("missing manifest fixture")
			}
			// Mutate a fresh asset after approval, and independently a returned Plan.
			tc.change(asset)
			if _, err = New(input).Plan(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("fresh Plan=%v", err)
			}
			fresh := filepath.Join(root, "fresh")
			if _, err = New(input).Generate(fresh); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("fresh Generate=%v", err)
			}
			if _, err = os.Stat(fresh); !os.IsNotExist(err) {
				t.Fatalf("fresh failure wrote destination: %v", err)
			}
			hook := plan.HookScaffold
			changed := &HookScaffoldAsset{File: hook.File, EntityHooks: hook.EntityHooks, evidence: hook.evidence}
			tc.change(changed)
			hook.File, hook.EntityHooks, hook.evidence = changed.File, changed.EntityHooks, changed.evidence
			for _, dir := range []string{fresh, existing} {
				if err = plan.ValidateDestination(dir); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("ValidateDestination=%v", err)
				}
				if _, err = EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("EmitScaffold=%v", err)
				}
			}
			if _, err = os.Stat(fresh); !os.IsNotExist(err) {
				t.Fatalf("plan failure wrote destination: %v", err)
			}
			after := map[string][]byte{}
			entries, err = os.ReadDir(existing)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					after[entry.Name()], err = os.ReadFile(filepath.Join(existing, entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rejected plan changed package/user files or manifest")
			}
		})
	}
}

func TestMutationScaffoldCloneOwnsEvidenceAndAST(t *testing.T) {
	asset := testMutationScaffoldAsset(t)
	// Retention must copy the compiler's native result, not just the map.
	retained := *asset
	retained.evidence = nil
	methods := map[string][]xshape.Method{asset.EntityHooks[0].Type: asset.evidence.roles[0].methods}
	if err := retained.RetainEntityHookEvidence(methods); err != nil {
		t.Fatal(err)
	}
	copy, err := asset.Clone()
	if err != nil {
		t.Fatal(err)
	}
	// Both native method slices and public role slices must be detached, as must
	// the retained approval versus the mutable AST and public metadata.
	asset.evidence.roles[0].methods[0].Parameters[0] = "broken"
	asset.evidence.roles[0].methods[0].Results[0] = "broken"
	asset.evidence.roles[0].contract.Path[0] = "broken"
	asset.evidence.roles[0].contract.Required[0] = "broken"
	asset.EntityHooks[0].Path[0] = "broken"
	asset.EntityHooks[0].Required[0] = "broken"
	asset.File.Name.Name = "broken"
	if err = copy.validateMutationMethods(""); err != nil {
		t.Fatalf("asset clone aliased evidence or source: %v", err)
	}
	if !reflect.DeepEqual(retained.evidence, copy.evidence) {
		t.Fatal("retention aliased native evidence inputs")
	}
	if err = copy.RetainEntityHookEvidence(methods); err == nil {
		t.Fatal("retained evidence was replaced")
	}
	generator := New(Input{Component: customHandlerComponent(), TargetPackage: copy.PackagePath, MutationHandler: mutationAsset(t, mutationDefinitionFixture), HookScaffold: copy})
	first, err := generator.Plan()
	if err != nil {
		t.Fatal(err)
	}
	first.HookScaffold.File.Name.Name = "changed"
	first.HookScaffold.EntityHooks[0].Path[0] = "changed"
	first.HookScaffold.evidence.roles[0].methods[0].Parameters[0] = "changed"
	second, err := generator.Plan()
	if err != nil {
		t.Fatalf("returned plan aliased generator AST/evidence: %v", err)
	}
	if err = second.validateMutationHookScaffold(); err != nil {
		t.Fatal(err)
	}
}
