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

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	smodel "github.com/viant/x/syntetic/model"
)

type descriptorAssetLeaf struct {
	Id int `sqlx:"id"`
}
type descriptorAssetChild struct {
	Id      int                    `sqlx:"id"`
	Details []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" view:",table=details" sql:"uri=queries/details.sql"`
}
type descriptorAssetRoot struct {
	Id      int                     `sqlx:"id"`
	Left    []*descriptorAssetChild `sqlx:"-" on:"Id:id=Id:id" view:",table=children" sql:"uri=queries/children.sql"`
	Right   *descriptorAssetChild   `sqlx:"-" on:"Id:id=Id:id" view:",table=children" sql:"uri=queries/children.sql"`
	Ignored string                  `sql:"uri=queries/not-a-relation.sql"`
}
type descriptorAssetCycle struct {
	Children []*descriptorAssetCycle `sqlx:"-" on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetEmbedded struct{ *descriptorAssetRoot }

type descriptorAssetSimple struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" view:",table=children" sql:"uri=queries/children.sql"`
}
type descriptorAssetTraversal struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" sql:"uri=../children.sql"`
}
type descriptorAssetInvalid struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" sql:"uri=queries//children.sql"`
}
type descriptorAssetScalar struct {
	Children int `on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetMap struct {
	Children map[string]*descriptorAssetLeaf `on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetArray struct {
	Children [1]*descriptorAssetLeaf `on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetDoublePointer struct {
	Children **descriptorAssetLeaf `on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetScalarMapping struct {
	Children []*descriptorAssetLeaf `sqlx:"children" on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetCodec struct {
	Children []*descriptorAssetLeaf `sqlx:"-" codec:"other" on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetExternal struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" sql:"uri=foreign:missing.sql"`
}
type descriptorAssetViewOnly struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" view:",uri=queries/children.sql"`
}
type descriptorAssetSQLOverride struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" view:",uri=missing.sql" sql:"uri=queries/children.sql"`
}
type descriptorAssetInline struct {
	Children []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" view:",uri=consumer:missing.sql" sql:"SELECT 1"`
}
type descriptorAssetSQLOnly struct {
	SQLOnly string `sql:"uri=missing.sql"`
}
type descriptorAssetNamespaceChild struct {
	Details []*descriptorAssetLeaf `sqlx:"-" on:"Id:id=Id:id" sql:"uri=consumer:queries/details.sql"`
}
type descriptorAssetReentry struct {
	Children []*descriptorAssetNamespaceChild `sqlx:"-" on:"Id:id=Id:id" sql:"uri=foreign:missing.sql"`
}

// These are isolated collector boundary controls. The separate stock Discovery
// fixture establishes genuine selected source/catalog and runtime authority.
func descriptorAssetResolver(t *testing.T) (*planResolver, string, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/viant/datly\n\ngo 1.25.8\n"), 0600); err != nil {
		t.Fatal(err)
	}
	pkg := reflect.TypeFor[descriptorAssetRoot]().PkgPath()
	dir := filepath.Join(root, "transcribe/generate")
	if err := os.MkdirAll(filepath.Join(dir, "queries"), 0755); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{"queries/children.sql": "SELECT id FROM children\n", "queries/details.sql": "SELECT id FROM details\n"} {
		if err := os.WriteFile(filepath.Join(dir, path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &planResolver{input: Input{ProjectRoot: root}, types: resolver, plan: &Plan{Generation: &spec.GenerationSettings{}}}, pkg, dir
}
func descriptorAssetShape(typ reflect.Type, pkg string) *xshape.Type {
	return xshape.New(x.NewType(typ, x.WithPkgPath(pkg)), nil)
}
func TestLinkedDescriptorAssetsNestedAndReusedRows(t *testing.T) {
	r, pkg, _ := descriptorAssetResolver(t)
	files := map[string]string{}
	resources := &ResourcePlan{Namespace: "consumer"}
	err := r.linkedDescriptorRelationAssets(resources, files, nil, descriptorAssetShape(reflect.TypeFor[descriptorAssetRoot](), pkg), resources.Namespace, nil, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files["queries/children.sql"] != "SELECT id FROM children\n" || files["queries/details.sql"] != "SELECT id FROM details\n" {
		t.Fatalf("assets=%v", files)
	}
}
func TestLinkedDescriptorAssetFailures(t *testing.T) {
	for _, mode := range []string{"missing", "traversal", "invalid", "conflict", "destination", "authored-missing", "authored-changed", "wrong-package", "no-project", "cycle", "scalar-relation", "map-relation", "array-relation", "double-pointer", "codec-mapping", "promoted", "symlink-escape"} {
		t.Run(mode, func(t *testing.T) {
			r, pkg, dir := descriptorAssetResolver(t)
			files := map[string]string{}
			retained := map[string]string{}
			resources := &ResourcePlan{Namespace: "consumer"}
			typ := reflect.TypeFor[descriptorAssetSimple]()
			uri := "queries/children.sql"
			switch mode {
			case "missing":
				if err := os.Remove(filepath.Join(dir, uri)); err != nil {
					t.Fatal(err)
				}
			case "traversal":
				typ = reflect.TypeFor[descriptorAssetTraversal]()
			case "invalid":
				typ = reflect.TypeFor[descriptorAssetInvalid]()
			case "conflict":
				files[uri] = "unrelated canonical query"
			case "destination":
				if err := r.plan.Generation.SetSQLFile("Children", "other.sql"); err != nil {
					t.Fatal(err)
				}
			case "authored-missing":
				resources.sourceText = "package source\n"
			case "authored-changed":
				resources.sourceText = "package source\n"
				retained[uri] = "changed original"
			case "wrong-package":
				pkg = "example.com/foreign/rows"
			case "no-project":
				r.input.ProjectRoot = ""
			case "cycle":
				typ = reflect.TypeFor[descriptorAssetCycle]()
			case "scalar-relation":
				typ = reflect.TypeFor[descriptorAssetScalar]()
			case "map-relation":
				typ = reflect.TypeFor[descriptorAssetMap]()
			case "array-relation":
				typ = reflect.TypeFor[descriptorAssetArray]()
			case "double-pointer":
				typ = reflect.TypeFor[descriptorAssetDoublePointer]()
			case "codec-mapping":
				typ = reflect.TypeFor[descriptorAssetCodec]()
			case "promoted":
				typ = reflect.TypeFor[descriptorAssetEmbedded]()
			case "symlink-escape":
				outside := filepath.Join(t.TempDir(), "outside.sql")
				if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, uri)); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(dir, uri)); err != nil {
					t.Fatal(err)
				}
			}
			err := r.linkedDescriptorRelationAssets(resources, files, retained, descriptorAssetShape(typ, pkg), resources.Namespace, nil, map[string]bool{})
			if err == nil {
				t.Fatalf("unsupported %s accepted: assets=%v", mode, files)
			}
		})
	}
}
func TestLinkedDescriptorAssetsPreserveCanonicalAndExternalOwners(t *testing.T) {
	for _, mode := range []string{"canonical-covered", "external-namespace", "authored-exact", "scalar-sql", "namespace-reentry", "view-only-uri", "sql-overrides-view", "inline-retains-namespace", "on-overrides-sqlx"} {
		t.Run(mode, func(t *testing.T) {
			r, pkg, _ := descriptorAssetResolver(t)
			resources := &ResourcePlan{Namespace: "consumer"}
			files := map[string]string{}
			covered := map[string]bool{}
			retained := map[string]string{}
			uri := "queries/children.sql"
			typ := reflect.TypeFor[descriptorAssetSimple]()
			switch mode {
			case "canonical-covered":
				covered["Children"] = true
			case "external-namespace":
				typ = reflect.TypeFor[descriptorAssetExternal]()
			case "authored-exact":
				resources.sourceText = "package source\n"
				retained[uri] = "SELECT id FROM children\n"
			case "scalar-sql":
				typ = reflect.TypeFor[descriptorAssetSQLOnly]()
			case "namespace-reentry":
				typ = reflect.TypeFor[descriptorAssetReentry]()
			case "view-only-uri":
				typ = reflect.TypeFor[descriptorAssetViewOnly]()
			case "sql-overrides-view":
				typ = reflect.TypeFor[descriptorAssetSQLOverride]()
			case "inline-retains-namespace":
				typ = reflect.TypeFor[descriptorAssetInline]()
			case "on-overrides-sqlx":
				typ = reflect.TypeFor[descriptorAssetScalarMapping]()
			}
			err := r.linkedDescriptorRelationAssets(resources, files, retained, descriptorAssetShape(typ, pkg), resources.Namespace, covered, map[string]bool{})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "authored-exact" {
				if files[uri] != retained[uri] {
					t.Fatal("source bytes changed")
				}
			} else if mode == "namespace-reentry" {
				if len(files) != 1 || files["queries/details.sql"] != "SELECT id FROM details\n" {
					t.Fatalf("namespace reentry missing: %v", files)
				}
			} else if mode == "view-only-uri" || mode == "sql-overrides-view" || mode == "on-overrides-sqlx" {
				if len(files) != 1 || files[uri] != "SELECT id FROM children\n" {
					t.Fatalf("view/sql URI precedence changed: %v", files)
				}
			} else if len(files) != 0 {
				t.Fatalf("owner changed: %v", files)
			}
		})
	}
}
func TestLinkedDescriptorResourceRequiresExactPackageAndFile(t *testing.T) {
	r, pkg, _ := descriptorAssetResolver(t)
	for _, bad := range []string{"", "transcribe/generate", "github.com/viant/datly/../escape", "example.com/foreign"} {
		if _, err := r.linkedDescriptorSQLResource(bad, "queries/children.sql"); err == nil {
			t.Fatalf("package %q accepted", bad)
		}
	}
	if _, err := r.linkedDescriptorSQLResource(pkg, "queries/missing.sql"); err == nil || !strings.Contains(err.Error(), "missing.sql") {
		t.Fatalf("missing asset fallback: %v", err)
	}
}

func TestLinkedDescriptorAssetsRejectAliasDeclaringPackage(t *testing.T) {
	r, pkg, dir := descriptorAssetResolver(t)
	// Even an existing same-relative local asset is not authority to relocate a
	// foreign alias declaration's SQL. Source AST is deliberately inconsistent
	// with a reflected facade here: this is a fail-closed metadata unit control,
	// not the stock selected-source positive proof.
	for _, declaration := range []string{"type Alias = foreign.Row", "type Alias foreign.Row"} {
		t.Run(declaration, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, "alias.go"), "package generate\n"+declaration, 0)
			if err != nil {
				t.Fatal(err)
			}
			spec := parsed.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec)
			descriptor := x.NewType(reflect.TypeFor[descriptorAssetSimple](), x.WithName("Alias"), x.WithPkgPath(pkg))
			descriptor.SynteticType = &smodel.Type{Name: "Alias", PkgPath: pkg, TypeSpec: spec}
			err = r.linkedDescriptorRelationAssets(&ResourcePlan{Namespace: "consumer"}, map[string]string{}, nil, xshape.New(descriptor, r.types.Descriptor), "consumer", nil, map[string]bool{})
			if err == nil {
				t.Fatal("alias copied a foreign declaration resource from local package")
			}
		})
	}
	// A scalar/no-relation descriptor is outside this resource extension.
	descriptor := x.NewType(reflect.TypeFor[descriptorAssetSQLOnly](), x.WithName("Alias"), x.WithPkgPath(pkg))
	descriptor.SynteticType = &smodel.Type{TypeSpec: &ast.TypeSpec{Type: ast.NewIdent("string"), Assign: token.Pos(1)}}
	if err := r.linkedDescriptorRelationAssets(&ResourcePlan{Namespace: "consumer"}, map[string]string{}, nil, xshape.New(descriptor, r.types.Descriptor), "consumer", nil, map[string]bool{}); err != nil {
		t.Fatalf("unrelated alias changed: %v", err)
	}
}

type descriptorAssetLinkedInput struct {
	Current []*descriptorAssetRoot `parameter:"Current,kind=view,in=Current" sql:"uri=foreign:root.sql"`
}
type descriptorAssetLinkedOutput struct {
	Data []*descriptorAssetRoot `parameter:"Data,kind=output,in=view" sql:"uri=foreign:root.sql"`
}

func TestLinkedDescriptorRootUsesEffectiveResourceNamespace(t *testing.T) {
	for _, mode := range []string{"compiled-foreign", "generated-input-rebase", "linked-input-actual-uri", "generated-output-rebase", "linked-output-actual-uri", "unmatched-contract", "ambiguous-binding"} {
		t.Run(mode, func(t *testing.T) {
			r, pkg, _ := descriptorAssetResolver(t)
			root := &spec.View{Name: "Root", Source: &spec.ViewSource{URI: "foreign:root.sql", Table: "parents"}}
			parameter := &spec.Parameter{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "Current"}}
			r.input.Component = &spec.Component{RootView: root}
			r.plan.Package = pkg
			catalog := typecatalog.NewCatalog()
			row := x.NewType(reflect.TypeFor[descriptorAssetRoot]())
			in := x.NewType(reflect.TypeFor[descriptorAssetLinkedInput]())
			out := x.NewType(reflect.TypeFor[descriptorAssetLinkedOutput]())
			if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, row, in, out); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			r.types = resolver
			planned := ViewPlan{Identity: "linked-root", DescriptorKey: row.Key()}
			want := "foreign"
			switch mode {
			case "generated-input-rebase", "linked-input-actual-uri", "ambiguous-binding":
				r.input.Component.Parameters = []*spec.Parameter{parameter}
				r.input.ViewBindings = ViewBindings{parameter.Identity(): planned.Identity}
				r.plan.Input = ContractPlan{Package: pkg, Ownership: ContractGenerated, Fields: []Field{{Name: "Current", Tag: `parameter:"Current,kind=view,in=Current" sql:"uri=consumer:root.sql"`}}}
				if mode == "generated-input-rebase" {
					want = "consumer"
				}
				if mode == "linked-input-actual-uri" {
					r.plan.Input.Ownership = ContractLinked
					r.plan.Input.DescriptorKey = in.Key()
				}
				if mode == "ambiguous-binding" {
					second := &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "view", Name: "Other"}}
					r.input.Component.Parameters = append(r.input.Component.Parameters, second)
					r.input.ViewBindings[second.Identity()] = planned.Identity
					r.plan.Input.Fields = append(r.plan.Input.Fields, Field{Name: "Other", Tag: `sql:"uri=consumer:other.sql"`})
				}
			case "generated-output-rebase", "linked-output-actual-uri":
				r.plan.Output = ContractPlan{Package: pkg, Ownership: ContractGenerated, Fields: []Field{{Name: "Data", Type: "[]*" + row.Key(), Tag: `parameter:"Data,kind=output,in=view" sql:"uri=consumer:root.sql"`}}}
				if mode == "generated-output-rebase" {
					want = "consumer"
				} else {
					r.plan.Output.Ownership = ContractLinked
					r.plan.Output.DescriptorKey = out.Key()
				}
			case "unmatched-contract":
				r.plan.Input.Fields = []Field{{Name: "Unmatched"}}
			}
			namespace, err := r.linkedDescriptorRootNamespace(&ResourcePlan{Namespace: "consumer"}, planned, root)
			if mode == "unmatched-contract" || mode == "ambiguous-binding" {
				if err == nil {
					t.Fatal("unproved namespace authority accepted")
				}
				return
			}
			if err != nil || namespace != want {
				t.Fatalf("namespace=%q want=%q err=%v", namespace, want, err)
			}
			files := map[string]string{}
			if err = r.linkedDescriptorRelationAssets(&ResourcePlan{Namespace: "consumer"}, files, nil, xshape.New(row, r.types.Descriptor), namespace, nil, map[string]bool{}); err != nil {
				t.Fatal(err)
			}
			if want == "foreign" && len(files) != 0 {
				t.Fatalf("foreign root copied descendants into consumer: %v", files)
			}
			if want == "consumer" && len(files) != 2 {
				t.Fatalf("rebased generated binding lost descendants: %v", files)
			}
		})
	}
}

type descriptorAssetMixedRoot struct {
	Children []*descriptorAssetChild `sqlx:"-" on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}
type descriptorAssetMixedReentryRoot struct {
	Children []*descriptorAssetNamespaceChild `sqlx:"-" on:"Id:id=Id:id" sql:"uri=queries/children.sql"`
}

func TestLinkedDescriptorAssetsMixedCanonicalNamespace(t *testing.T) {
	for _, reentry := range []bool{false, true} {
		name := "foreign-inheritance"
		if reentry {
			name = "explicit-consumer-reentry"
		}
		t.Run(name, func(t *testing.T) {
			r, _, _ := descriptorAssetResolver(t)
			rowType := reflect.TypeFor[descriptorAssetMixedRoot]()
			childType := reflect.TypeFor[descriptorAssetChild]()
			if reentry {
				rowType = reflect.TypeFor[descriptorAssetMixedReentryRoot]()
				childType = reflect.TypeFor[descriptorAssetNamespaceChild]()
			}
			catalog := typecatalog.NewCatalog()
			row := x.NewType(rowType)
			if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, row, x.NewType(childType), x.NewType(reflect.TypeFor[descriptorAssetLeaf]())); err != nil {
				t.Fatal(err)
			}
			var err error
			r.types, err = typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			child := &spec.View{Name: "Children", Source: &spec.ViewSource{SQL: "SELECT id FROM children\n"}}
			root := &spec.View{Name: "Root", Source: &spec.ViewSource{URI: "foreign:root.sql"}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: child}}}
			r.input.Component = &spec.Component{RootView: root}
			identity, err := root.Identity()
			if err != nil {
				t.Fatal(err)
			}
			r.plan.Views = []ViewPlan{{Identity: identity, Name: "Root", DescriptorKey: row.Key(), Ownership: ViewLinked}}
			files := map[string]string{}
			if err := r.linkedRelationResources(&ResourcePlan{Namespace: "consumer"}, files, nil); err != nil {
				t.Fatal(err)
			}
			// Existing canonical copying is deliberately unchanged. Only the new
			// descriptor grandchild follows the bound foreign root context.
			if files["queries/children.sql"] != "SELECT id FROM children\n" {
				t.Fatalf("canonical asset authority changed: %v", files)
			}
			_, found := files["queries/details.sql"]
			if found != reentry || len(files) != 1+btoi(reentry) {
				t.Fatalf("descriptor namespace reentry=%v files=%v", reentry, files)
			}
		})
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}
