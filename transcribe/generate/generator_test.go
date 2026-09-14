package generate

import (
	"errors"
	"github.com/viant/datly/internal/testharness"
	"go/ast"
	"go/parser"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestGenerator_ConsumesImmutableDeclarationPlan(t *testing.T) {
	param := &spec.Parameter{
		Name:   "EventTypes",
		Source: spec.BindSource{Kind: "param", Name: "Events"},
	}
	component := &spec.Component{Name: "Events", Parameters: []*spec.Parameter{param}}
	projection := []DeclarationProjection{{Name: "Price", Source: "Price"}, {Name: "Timestamp", Source: "Timestamp"}}
	declarations := Declarations{
		param.Identity(): {Projection: projection, NestedChildField: "EventsPerformance"},
	}
	generator := New(Input{Component: component, Declarations: declarations})
	projection[0].Name = "Changed"
	component.Name = "Changed"
	param.Name = "Changed"

	plan, err := (&planResolver{input: generator.input}).resolveBase()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.HelperTypes) != 1 || len(plan.HelperTypes[0].Fields) != 2 || plan.HelperTypes[0].Fields[0].Name != "Price" {
		t.Fatalf("HelperTypes = %+v", plan.HelperTypes)
	}
	if plan.ComponentName != "Events" {
		t.Fatalf("ComponentName = %q", plan.ComponentName)
	}
}

func TestGeneratorMaterializesDestinationsOnItsCanonicalSnapshot(t *testing.T) {
	child := &spec.View{Name: "Items"}
	component := &spec.Component{Name: "Orders", Settings: &spec.Settings{Generation: &spec.GenerationSettings{ViewFile: "orders.go"}}, RootView: &spec.View{
		Name: "Orders", Relations: []*spec.Relation{{Name: "Items", Holder: "Items", View: child}},
	}}
	plan, err := New(Input{Component: component}).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if component.RootView.Dest != "" || child.Dest != "" {
		t.Fatalf("generator mutated caller metadata: root=%q child=%q", component.RootView.Dest, child.Dest)
	}
	if plan.Views[0].Destination != "orders.go" || plan.Views[1].Destination != "orders.go" {
		t.Fatalf("effective destinations = %+v", plan.Views)
	}
	rootTag := reflect.StructTag(plan.Output.Fields[1].Tag).Get("view")
	if !strings.Contains(rootTag, "dest=orders.go") {
		t.Fatalf("root output tag = %q", rootTag)
	}
	childTag := reflect.StructTag(plan.Views[0].Fields[0].Tag).Get("view")
	if !strings.Contains(childTag, "dest=orders.go") {
		t.Fatalf("child relation tag = %q", childTag)
	}
}

func TestGenerator_RequiresComponent(t *testing.T) {
	if _, err := (*Generator)(nil).Plan(); err == nil || !strings.Contains(err.Error(), "generator is required") {
		t.Fatalf("nil Plan() error = %v", err)
	}
	if _, err := New(Input{}).Plan(); err == nil || !strings.Contains(err.Error(), "component is required") {
		t.Fatalf("empty Plan() error = %v", err)
	}
}

func TestGeneratorRejectsInvalidIndependentViewCardinality(t *testing.T) {
	component := &spec.Component{
		Name: "Users",
		Parameters: []*spec.Parameter{{
			Name: "Authorization", Source: spec.BindSource{Kind: "view", Name: "Authorization"}, Cardinality: "Some",
		}},
		Views: []*spec.View{{Name: "Authorization", Columns: []*spec.Column{{Name: "Authorized", Source: "authorized"}}}},
	}
	_, err := New(Input{Component: component}).Plan()
	if err == nil || !strings.Contains(err.Error(), `unsupported independent view cardinality "Some"`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorUsesExactIndependentViewBinding(t *testing.T) {
	authored := &spec.View{
		Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/app", Name: "Audit"},
		Name: "Audit", Namespace: "current", TypeName: "CurrentAudit",
	}
	preserved := &spec.View{
		Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/app", Name: "Audit"},
		Name: "Audit", Namespace: "archive", TypeName: "ArchiveAudit",
	}
	param := &spec.Parameter{Name: "Audit", Source: spec.BindSource{Kind: "view", Name: "Audit"}, Cardinality: "Many"}
	authoredIdentity, err := authored.Identity()
	if err != nil {
		t.Fatal(err)
	}
	bindings := ViewBindings{param.Identity(): authoredIdentity}
	generator := New(Input{
		Component:    &spec.Component{Name: "AuditLog", Parameters: []*spec.Parameter{param}, Views: []*spec.View{authored, preserved}},
		ViewBindings: bindings,
	})
	bindings[param.Identity()] = "changed"
	plan, err := generator.Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "[]*CurrentAudit" || len(plan.Views) != 2 {
		t.Fatalf("plan = views:%+v input:%+v", plan.Views, plan.Input.Fields)
	}
}

func TestGeneratorRejectsStaleIndependentViewBinding(t *testing.T) {
	param := &spec.Parameter{Name: "Audit", Source: spec.BindSource{Kind: "view", Name: "Audit"}, Cardinality: "Many"}
	_, err := New(Input{
		Component:    &spec.Component{Name: "AuditLog", Parameters: []*spec.Parameter{param}, Views: []*spec.View{{Name: "Audit"}}},
		ViewBindings: ViewBindings{param.Identity(): "missing"},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "targets unknown canonical view") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsSwappedIndependentViewBinding(t *testing.T) {
	audit := &spec.Parameter{Name: "Audit", Source: spec.BindSource{Kind: "view", Name: "Audit"}, Cardinality: "Many"}
	billing := &spec.Parameter{Name: "Billing", Source: spec.BindSource{Kind: "view", Name: "Billing"}, Cardinality: "Many"}
	auditView := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/contracts", Name: "Audit"}, Name: "Audit"}
	billingView := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/contracts", Name: "Billing"}, Name: "Billing"}
	billingIdentity, err := billingView.Identity()
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Input{
		Component:    &spec.Component{Parameters: []*spec.Parameter{audit, billing}, Views: []*spec.View{auditView, billingView}},
		ViewBindings: ViewBindings{audit.Identity(): billingIdentity},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), `targets view`) || !strings.Contains(err.Error(), `instead of "Audit"`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorPlansLinkedContractsWithoutDuplicateEmission(t *testing.T) {
	type linkedInput struct{}
	type linkedOutput struct{}
	catalog := typecatalog.NewCatalog()
	inputDescriptor := x.NewType(reflect.TypeOf(linkedInput{}), x.WithName("Input"), x.WithPkgPath("example.com/contracts"))
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/contracts"))
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, inputDescriptor, outputDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := New(Input{
		Component: &spec.Component{Name: "Users"}, TypeResolver: resolver, TargetPackage: "example.com/generated/users",
		Contracts: ContractReferences{
			Input:  &ContractReference{Expression: "*Input", DescriptorKey: inputDescriptor.Key()},
			Output: &ContractReference{Expression: "[]*Output", DescriptorKey: outputDescriptor.Key()},
		},
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Input.Ownership != ContractLinked || plan.Input.Type != "*contracts.Input" || plan.Input.Destination != "users_input.go" ||
		plan.Output.Ownership != ContractLinked || plan.Output.Type != "[]*contracts.Output" || plan.Output.Destination != "users_output.go" {
		t.Fatalf("linked contracts = input:%+v output:%+v", plan.Input, plan.Output)
	}
	content, err := componentFileText("users", plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(content, `contracts "example.com/contracts"`) != 1 ||
		!strings.Contains(content, "xdatly.Component[*contracts.Input, []*contracts.Output]") {
		t.Fatalf("linked component source:\n%s", content)
	}
}

func TestGeneratorRemovesStaleGeneratedContractsWhenRolesBecomeLinked(t *testing.T) {
	type linkedInput struct{}
	type linkedOutput struct{}
	dir := t.TempDir()
	component := &spec.Component{Name: "Users"}
	if _, err := New(Input{Component: component}).Generate(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"users_input.go", "users_output.go"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("generated contract %s missing: %v", name, err)
		}
	}
	catalog := typecatalog.NewCatalog()
	inputDescriptor := x.NewType(reflect.TypeOf(linkedInput{}), x.WithName("Input"), x.WithPkgPath("example.com/contracts"))
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/contracts"))
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, inputDescriptor, outputDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = New(Input{
		Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated/users",
		Contracts: ContractReferences{
			Input:  &ContractReference{Expression: "Input", DescriptorKey: inputDescriptor.Key()},
			Output: &ContractReference{Expression: "Output", DescriptorKey: outputDescriptor.Key()},
		},
	}).Generate(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"users_input.go", "users_output.go"} {
		if _, err = os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("stale generated contract %s remains: %v", name, err)
		}
	}
	if _, err = os.Stat(filepath.Join(dir, scaffoldManifestName)); err != nil {
		t.Fatalf("generation manifest missing: %v", err)
	}
}

func TestGeneratorEmitsOnlyDivergedContractRole(t *testing.T) {
	type linkedOutput struct{}
	catalog := typecatalog.NewCatalog()
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/contracts"))
	if err := catalog.Register(typecatalog.TypeOriginPackage, outputDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := New(Input{
		Component: &spec.Component{Name: "Users"}, TypeResolver: resolver, TargetPackage: "example.com/generated/users",
		Contracts: ContractReferences{Output: &ContractReference{Expression: "Output", DescriptorKey: outputDescriptor.Key()}},
	}).Generate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if result.Plan.Input.Ownership != ContractGenerated || result.Plan.Output.Ownership != ContractLinked || len(result.Files) != 2 {
		t.Fatalf("partial contract result = %+v", result)
	}
	for _, file := range result.Files {
		if strings.HasSuffix(file.Path, "users_output.go") {
			t.Fatalf("linked output was emitted: %s", file.Path)
		}
	}
}

func TestGeneratorLinksPackageOwnedRootViewWithoutDuplicateEmission(t *testing.T) {
	type linkedOutput struct{}
	type linkedRow struct{ ID int }
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	contractsDir := filepath.Join(root, "contracts")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "contracts.go"), []byte("package contracts\n\ntype Row struct { ID int }\ntype Output struct { Data []*Row }\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	catalog := typecatalog.NewCatalog()
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/generated/contracts"))
	rowDescriptor := x.NewType(reflect.TypeOf(linkedRow{}), x.WithName("Row"), x.WithPkgPath("example.com/generated/contracts"))
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, outputDescriptor, rowDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Name: "Users", RootView: &spec.View{
		Name: "Users", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}},
	}}
	packageDir := filepath.Join(root, "users")
	if _, err = New(Input{Component: component}).Generate(packageDir); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(packageDir, "users.go")); err != nil {
		t.Fatalf("initial generated view missing: %v", err)
	}

	references := ViewReferences{RootViewPath: {DescriptorKey: rowDescriptor.Key()}}
	generator := New(Input{
		Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated/users",
		Contracts: ContractReferences{Output: &ContractReference{Expression: "Output", DescriptorKey: outputDescriptor.Key()}},
		Views:     references,
	})
	references[RootViewPath].DescriptorKey = "changed"
	result, err := generator.Generate(packageDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan.Views) != 1 || result.Plan.Views[0].Ownership != ViewLinked ||
		result.Plan.Views[0].Type != "contracts.Row" || result.Plan.Views[0].DescriptorKey != rowDescriptor.Key() ||
		result.Plan.RootViewType != "contracts.Row" {
		t.Fatalf("linked view plan = %+v", result.Plan.Views)
	}
	if _, err = os.Stat(filepath.Join(packageDir, "users.go")); err != nil {
		t.Fatalf("previous generated shape was removed: %v", err)
	}
	for _, file := range result.Files {
		if strings.HasSuffix(file.Path, "users.go") || strings.Contains(file.Content, "type Row struct") {
			t.Fatalf("linked view was emitted: %+v", file)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("linked-view package did not compile: %v\n%s", err, output)
	}
}

func TestGeneratorPlansIndependentViewAsTypedInput(t *testing.T) {
	component := &spec.Component{
		Name: "Users",
		Parameters: []*spec.Parameter{{
			Name: "Authorization", Source: spec.BindSource{Kind: "view", Name: "Authorization"}, TypeExpr: "*Authorization",
		}},
		Views: []*spec.View{{
			Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/generated/users", Name: "Authorization"},
			Name: "Authorization", Columns: []*spec.Column{{Name: "authorized", Type: spec.TypeRef{Name: "bool"}}},
		}},
	}
	plan, err := New(Input{Component: component, TargetPackage: "example.com/generated/users"}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Views) != 1 || plan.Views[0].Name != "Authorization" || plan.Views[0].Ownership != ViewGenerated ||
		len(plan.Views[0].Fields) != 1 || plan.Views[0].Fields[0].Name != "Authorized" {
		t.Fatalf("independent view plan = %+v", plan.Views)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "*Authorization" {
		t.Fatalf("independent view input = %+v", plan.Input.Fields)
	}
	if placeholders := plan.referencedPlaceholderTypes(); len(placeholders) != 0 {
		t.Fatalf("independent view emitted placeholders = %v", placeholders)
	}
}

func TestGeneratorLinksPackageOwnedIndependentView(t *testing.T) {
	type linkedRow struct{ ID int }
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	contractsDir := filepath.Join(root, "contracts")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "contracts.go"), []byte("package contracts\n\ntype Row struct { ID int }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	descriptor := x.NewType(reflect.TypeOf(linkedRow{}), x.WithName("Row"), x.WithPkgPath("example.com/generated/contracts"))
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/generated/contracts", Name: "Existing"}, Name: "Existing"}
	identity, err := view.Identity()
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Name:       "Users",
		Parameters: []*spec.Parameter{{Name: "Existing", Source: spec.BindSource{Kind: "view", Name: "Existing"}, TypeExpr: "[]*Row"}},
		Views:      []*spec.View{view},
	}
	result, err := New(Input{
		Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated/users",
		Views: ViewReferences{identity: {DescriptorKey: descriptor.Key()}},
	}).Generate(filepath.Join(root, "users"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan.Views) != 1 || result.Plan.Views[0].Ownership != ViewLinked || result.Plan.Views[0].Type != "contracts.Row" ||
		len(result.Plan.Input.Fields) != 1 || result.Plan.Input.Fields[0].Type != "[]*contracts.Row" {
		t.Fatalf("linked independent view = views:%+v input:%+v", result.Plan.Views, result.Plan.Input.Fields)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("linked independent-view module did not compile: %v\n%s", runErr, output)
	}
}

func TestGeneratorRejectsAmbiguousIndependentViewBinding(t *testing.T) {
	component := &spec.Component{
		Name:       "Users",
		Parameters: []*spec.Parameter{{Name: "Audit", Source: spec.BindSource{Kind: "view", Name: "Audit"}, TypeExpr: "[]*Audit"}},
		Views: []*spec.View{
			{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/generated/users", Name: "Audit"}, Name: "Audit", Namespace: "current", TypeName: "CurrentAudit"},
			{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/generated/users", Name: "Audit"}, Name: "Audit", Namespace: "archive", TypeName: "ArchivedAudit"},
		},
	}
	_, err := New(Input{Component: component}).Plan()
	if err == nil || !strings.Contains(err.Error(), "matches more than one independent view") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsInvalidLinkedRootViewAuthority(t *testing.T) {
	type linkedOutput struct{}
	catalog := typecatalog.NewCatalog()
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/contracts"))
	if err := catalog.Register(typecatalog.TypeOriginPackage, outputDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Name: "Users", RootView: &spec.View{Name: "Users"}}
	contract := ContractReferences{Output: &ContractReference{Expression: "Output", DescriptorKey: outputDescriptor.Key()}}
	for _, testCase := range []struct {
		name     string
		resolver *typecatalog.Resolver
		views    ViewReferences
		want     string
	}{
		{name: "missing authority", views: ViewReferences{RootViewPath: {DescriptorKey: "example.com.Row"}}, want: "requires type authority"},
		{name: "empty key", resolver: resolver, views: ViewReferences{RootViewPath: {}}, want: "descriptor key is required"},
		{name: "missing descriptor", resolver: resolver, views: ViewReferences{RootViewPath: {DescriptorKey: "example.com.Row"}}, want: "not a named package type"},
		{name: "unknown independent view", resolver: resolver, views: ViewReferences{"root/child": {DescriptorKey: outputDescriptor.Key()}}, want: "has no canonical independent view"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(Input{Component: component, TypeResolver: testCase.resolver, Contracts: contract, Views: testCase.views}).Plan()
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Plan() error = %v", err)
			}
		})
	}
	_, err = New(Input{Component: component, TypeResolver: resolver, Views: ViewReferences{
		RootViewPath: {DescriptorKey: outputDescriptor.Key()},
	}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "requires a linked output contract") {
		t.Fatalf("Plan() error = %v", err)
	}
	_, err = New(Input{
		Component: &spec.Component{Name: "Users"}, TypeResolver: resolver, Contracts: contract,
		Views: ViewReferences{RootViewPath: {DescriptorKey: outputDescriptor.Key()}},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "requires canonical root view metadata") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsQualifiedGeneratedContractName(t *testing.T) {
	_, err := New(Input{Component: &spec.Component{
		Name: "Users", Settings: &spec.Settings{InputType: "models.Input"},
	}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "must be a local Go identifier") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGenerator_PropagatesFieldTypeResolutionErrorBeforeHelperProjection(t *testing.T) {
	param := &spec.Parameter{
		Name:     "Events",
		TypeExpr: "models.Event",
	}
	helper := &spec.Parameter{
		Name:   "EventTypes",
		Source: spec.BindSource{Kind: "param", Name: "Events"},
	}
	component := &spec.Component{Name: "Events", Parameters: []*spec.Parameter{param, helper}}
	declarations := Declarations{
		helper.Identity(): {Projection: []DeclarationProjection{{Name: "Price", Source: "Price"}}, NestedChildField: "Performance"},
	}
	generator := New(Input{Component: component, Declarations: declarations})
	generator.resolver = reflectResolver(func(string) (reflect.Type, error) {
		return nil, errors.New("catalog unavailable")
	})

	_, err := generator.Plan()
	if err == nil || !strings.Contains(err.Error(), "catalog unavailable") || !strings.Contains(err.Error(), "models.Event") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGenerator_DelegatesUnqualifiedResolutionToTypeContext(t *testing.T) {
	type customer struct{}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(customer{}), x.WithName("Customer"), x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		DefaultPackage: "example.com/missing",
		Imports:        []typecatalog.PackageImport{{Alias: "model", Package: "example.com/models"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Name: "Customers",
		TypeContext: &spec.TypeContext{
			DefaultPackage: "example.com/missing",
			Imports:        []spec.ImportSpec{{Alias: "model", Package: "example.com/models"}},
		},
		Parameters: []*spec.Parameter{{
			Name: "Customer", Source: spec.BindSource{Kind: "body", Name: "customer"},
			Tag: `typeName:"Customer"`,
		}},
	}
	plan, err := New(Input{Component: component, TypeResolver: resolver}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "model.Customer" {
		t.Fatalf("InputFields = %+v", plan.Input.Fields)
	}
}

func TestGenerator_UnresolvedExternalDefaultPackageTypeFailsClosed(t *testing.T) {
	component := &spec.Component{
		Name:        "Customers",
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/models"},
		Parameters: []*spec.Parameter{{
			Name: "Customer", Source: spec.BindSource{Kind: "body", Name: "customer"},
			Tag: `typeName:"Customer"`,
		}},
	}
	resolver := testTypeResolver(func(string) (*x.Type, error) { return nil, nil })
	generator := New(Input{Component: component})
	generator.resolver = resolver
	_, err := generator.Plan()
	if err == nil || !strings.Contains(err.Error(), `package type was not found`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGenerator_LocalDefaultPackageAllowsGeneratedType(t *testing.T) {
	component := &spec.Component{
		Name:        "Customers",
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/generated/customers"},
		Parameters: []*spec.Parameter{{
			Name: "Customer", Source: spec.BindSource{Kind: "body", Name: "customer"},
			Tag: `typeName:"Customer"`,
		}},
	}
	plan, err := New(Input{Component: component, TargetPackage: "example.com/generated/customers"}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "Customer" {
		t.Fatalf("input fields = %+v", plan.Input.Fields)
	}
}

func TestGenerator_ExplicitQualifiedTypeRequiresAuthority(t *testing.T) {
	component := &spec.Component{
		Name: "Customers",
		Parameters: []*spec.Parameter{{
			Name: "Customer", TypeExpr: "models.Customer",
			Source: spec.BindSource{Kind: "body", Name: "customer"},
		}},
	}
	_, err := New(Input{Component: component}).Plan()
	if err == nil || !strings.Contains(err.Error(), `package type requires type authority`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGenerator_RewritesExplicitInputAndOutputTypesFromExternalDefaultPackage(t *testing.T) {
	type customer struct{}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(customer{}), x.WithName("Customer"), x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{DefaultPackage: "example.com/models"})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Name:        "Customers",
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/models"},
		Parameters: []*spec.Parameter{
			{Name: "Customer", TypeExpr: "*Customer", Source: spec.BindSource{Kind: "body", Name: "customer"}},
			{Name: "Items", OutputTypeExpr: "[]Customer", EmitOutput: true, Source: spec.BindSource{Kind: "output", Name: "items"}},
		},
	}
	plan, err := New(Input{
		Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated/customers",
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "*models.Customer" {
		t.Fatalf("input fields = %+v", plan.Input.Fields)
	}
	if len(plan.Output.Fields) != 1 || plan.Output.Fields[0].Type != "[]models.Customer" {
		t.Fatalf("output fields = %+v", plan.Output.Fields)
	}
	if len(plan.Imports) != 1 || plan.Imports[0] != (spec.ImportSpec{Alias: "models", Package: "example.com/models"}) {
		t.Fatalf("imports = %+v", plan.Imports)
	}
	if source := viewFile("customers", plan); strings.Contains(source, "type Customer struct{}") {
		t.Fatalf("resolved package type leaked a local placeholder:\n%s", source)
	}
}

func TestGenerator_RewritesQualifiedTypesInsideComposite(t *testing.T) {
	type customer struct{}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(customer{}), x.WithName("Customer"), x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		Imports: []typecatalog.PackageImport{{Alias: "model", Package: "example.com/models"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Name: "Customers",
		TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{
			Alias: "model", Package: "example.com/models",
		}}},
		Parameters: []*spec.Parameter{{
			Name: "Customers", TypeExpr: "map[string][]*model.Customer",
			Source: spec.BindSource{Kind: "body", Name: "customers"},
		}},
	}
	plan, err := New(Input{
		Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated/customers",
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "map[string][]*model.Customer" {
		t.Fatalf("input fields = %+v", plan.Input.Fields)
	}
}

func TestGenerator_FullPackagePathWithoutAuthorityFailsClosed(t *testing.T) {
	component := &spec.Component{
		Name: "Customers",
		Parameters: []*spec.Parameter{{
			Name: "Customer", Source: spec.BindSource{Kind: "body", Name: "customer"},
			Tag: `typeName:"example.com/models.Customer"`,
		}},
	}
	_, err := New(Input{Component: component}).Plan()
	if err == nil || !strings.Contains(err.Error(), `package type requires type authority`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGenerator_BuiltinTypeDoesNotRequireExternalDefaultPackageLookup(t *testing.T) {
	component := &spec.Component{
		Name:        "Customers",
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/models"},
		Parameters: []*spec.Parameter{{
			Name: "Search", TypeExpr: "map[string][]int",
			Source: spec.BindSource{Kind: "query", Name: "search"},
		}},
	}
	plan, err := New(Input{Component: component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "map[string][]int" {
		t.Fatalf("input fields = %+v", plan.Input.Fields)
	}
}

func TestGeneratorAllocatesUniqueAliasForResolvedDescriptor(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.RegisterPackage(typecatalog.TypeOriginGenerated, &smodel.Package{
		Name: "models", PkgPath: "example.net/second/models",
		Types: []*smodel.Type{syntheticTypeInPackage(t, "example.net/second/models", "Customer", "struct{}")},
	}); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Name: "Customers",
		TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{
			Alias: "models", Package: "example.com/first/models",
		}}},
		Parameters: []*spec.Parameter{{
			Name: "Customer", Source: spec.BindSource{Kind: "body", Name: "customer"},
			Tag: `typeName:"example.net/second/models.Customer"`,
		}},
	}
	plan, err := New(Input{Component: component, TypeResolver: resolver}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Type != "models2.Customer" {
		t.Fatalf("InputFields = %+v", plan.Input.Fields)
	}
	content := structFileWithImports("customers", "// input", "Input", plan.Input.Fields, plan.Imports)
	if !strings.Contains(content, `models2 "example.net/second/models"`) || strings.Contains(content, `models "example.net/second/models"`) {
		t.Fatalf("generated imports do not preserve alias identity:\n%s", content)
	}
}

func TestPackageAliasProducesValidLeadingRune(t *testing.T) {
	for _, testCase := range []struct {
		path string
		want string
	}{
		{path: "example.com/2models", want: "_2models"},
		{path: "example.com/-9models", want: "_9models"},
		{path: "example.com/-", want: "pkg"},
		{path: "example.com/type", want: "pkg"},
	} {
		if actual := packageAlias(testCase.path); actual != testCase.want {
			t.Errorf("packageAlias(%q) = %q, want %q", testCase.path, actual, testCase.want)
		}
	}
}

func TestImportsForFieldsUsesExactASTQualifier(t *testing.T) {
	imports := []spec.ImportSpec{
		{Alias: "foo", Package: "example.com/foo"},
		{Alias: "myfoo", Package: "example.com/myfoo"},
	}
	actual := importsForFields([]Field{{Type: "[]*myfoo.Record"}}, imports)
	if len(actual) != 1 || actual[0] != imports[1] {
		t.Fatalf("importsForFields() = %+v", actual)
	}
}

func TestGeneratorConcretizesHelperFieldsFromSyntheticType(t *testing.T) {
	event := syntheticType(t, "Event", `struct { EventsPerformance []Performance }`)
	performance := syntheticType(t, "Performance", `struct { Price float64; Timestamp string; Notes []string }`)
	catalog := typecatalog.NewCatalog()
	if err := catalog.RegisterPackage(typecatalog.TypeOriginGenerated, &smodel.Package{
		Name: "models", PkgPath: "example.com/models", Types: []*smodel.Type{event, performance},
	}); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		Imports: []typecatalog.PackageImport{{Alias: "models", Package: "example.com/models"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := &spec.Parameter{Name: "Events", TypeExpr: "*models.Event", Source: spec.BindSource{Kind: "body", Name: "events"}}
	helper := &spec.Parameter{Name: "EventTypes", Source: spec.BindSource{Kind: "param", Name: "Events"}}
	component := &spec.Component{
		Name:        "Events",
		TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "models", Package: "example.com/models"}}},
		Parameters:  []*spec.Parameter{events, helper},
	}
	declarations := Declarations{
		helper.Identity(): {Projection: []DeclarationProjection{{Name: "Price", Source: "Price"}, {Name: "Timestamp", Source: "Timestamp"}}, NestedChildField: "EventsPerformance"},
	}
	plan, err := New(Input{Component: component, Declarations: declarations, TypeResolver: resolver}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.HelperTypes) != 1 || len(plan.HelperTypes[0].Fields) != 2 ||
		plan.HelperTypes[0].Fields[0].Type != "float64" || plan.HelperTypes[0].Fields[1].Type != "string" {
		t.Fatalf("HelperTypes = %+v", plan.HelperTypes)
	}
}

func TestGeneratorRejectsCyclicSyntheticHelperType(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.RegisterPackage(typecatalog.TypeOriginGenerated, &smodel.Package{
		Name: "models", PkgPath: "example.com/models",
		Types: []*smodel.Type{syntheticType(t, "A", "B"), syntheticType(t, "B", "A")},
	}); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		Imports: []typecatalog.PackageImport{{Alias: "models", Package: "example.com/models"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &spec.Parameter{Name: "Source", TypeExpr: "models.A", Source: spec.BindSource{Kind: "body", Name: "source"}}
	helper := &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "param", Name: "Source"}}
	_, err = New(Input{
		Component: &spec.Component{Name: "Cycle", Parameters: []*spec.Parameter{source, helper}},
		Declarations: Declarations{helper.Identity(): {
			Projection: []DeclarationProjection{{Name: "Value", Source: "Value"}}, NestedChildField: "Child",
		}},
		TypeResolver: resolver,
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "cyclic synthetic type") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorUsesProjectionAliasAndSourceTypeForHelperField(t *testing.T) {
	type performance struct {
		Price float64
	}
	type event struct {
		Performance []*performance
	}
	events := &spec.Parameter{Name: "Events", TypeExpr: "[]*models.Event", Source: spec.BindSource{Kind: "body", Name: "Events"}}
	helper := &spec.Parameter{Name: "Prices", Source: spec.BindSource{Kind: "param", Name: "Events"}}
	component := &spec.Component{Name: "Events", Parameters: []*spec.Parameter{events, helper}}
	generator := New(Input{
		Component: component,
		Declarations: Declarations{helper.Identity(): {
			Projection: []DeclarationProjection{{Name: "CurrentPrice", Source: "Price"}}, NestedChildField: "Performance",
		}},
	})
	generator.resolver = reflectResolver(func(name string) (reflect.Type, error) {
		if name == "models.Event" || name == "Event" {
			return reflect.TypeOf(event{}), nil
		}
		return nil, nil
	})
	plan, err := generator.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.HelperTypes) != 1 || len(plan.HelperTypes[0].Fields) != 1 ||
		plan.HelperTypes[0].Fields[0] != (Field{Name: "CurrentPrice", Type: "float64"}) {
		t.Fatalf("helper projection = %+v", plan.HelperTypes)
	}
}

func TestGeneratorRejectsUnexportedProjectionAlias(t *testing.T) {
	helper := &spec.Parameter{Name: "Prices", Source: spec.BindSource{Kind: "param", Name: "Events"}}
	_, err := New(Input{
		Component: &spec.Component{Name: "Events", Parameters: []*spec.Parameter{helper}},
		Declarations: Declarations{helper.Identity(): {
			Projection: []DeclarationProjection{{Name: "currentPrice", Source: "Price"}},
		}},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), `projection alias "currentPrice" must be an exported Go identifier`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsDuplicateProjectionAlias(t *testing.T) {
	helper := &spec.Parameter{Name: "Prices", Source: spec.BindSource{Kind: "param", Name: "Events"}}
	_, err := New(Input{
		Component: &spec.Component{Name: "Events", Parameters: []*spec.Parameter{helper}},
		Declarations: Declarations{helper.Identity(): {Projection: []DeclarationProjection{
			{Name: "Value", Source: "Price"}, {Name: "Value", Source: "Cost"},
		}}},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), `projection alias "Value" is duplicated`) {
		t.Fatalf("Plan() error = %v", err)
	}
}

func syntheticType(t *testing.T, name, expression string) *smodel.Type {
	return syntheticTypeInPackage(t, "example.com/models", name, expression)
}

func syntheticTypeInPackage(t *testing.T, pkgPath, name, expression string) *smodel.Type {
	t.Helper()
	typ, err := parser.ParseExpr(expression)
	if err != nil {
		t.Fatal(err)
	}
	return &smodel.Type{Name: name, PkgPath: pkgPath, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(name), Type: typ}}
}
