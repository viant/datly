package bootstrap

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
)

const descriptorComponentSource = `package users

import xdatly "github.com/viant/xdatly"

type AuditRow struct {
	ID int ` + "`" + `sqlx:"ID"` + "`" + `
}

type UserRow struct {
	ID int ` + "`" + `sqlx:"ID"` + "`" + `
}

type UsersInput struct {
	Tenant string      ` + "`" + `parameter:"Tenant,kind=query,in=tenant"` + "`" + `
	Audit  []*AuditRow ` + "`" + `parameter:"Audit,kind=view,in=Audit" view:"Audit,table=audit"` + "`" + `
}

type UsersOutput struct {
	Data []*UserRow ` + "`" + `parameter:"Data,kind=output,in=view" view:"Users,table=users" sql:"SELECT ID FROM users"` + "`" + `
}

type Component struct {
	Contract xdatly.Component[UsersInput, UsersOutput] ` + "`" + `component:"Users,path=/v1/users,method=GET,view=Users"` + "`" + `
}
`

func TestDescriptorContractIdentityResolvesGenericOuterAlias(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	descriptor := &x.Type{PkgPath: "example.com/model", Name: "Page[example.com/other.Item]"}
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, descriptor,
		&x.Type{PkgPath: "example.com/other", Name: "Item"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{
		Imports: []typecatalog.PackageImport{
			{Alias: "model", Package: "example.com/model"},
			{Alias: "other", Package: "example.com/other"},
		},
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	actual, err := resolver.ResolveShape("[]*model.Page[other.Item]")
	if err != nil {
		t.Fatalf("ResolveShape() error = %v", err)
	}
	if actual == nil || actual.Identity != "[]*example.com/model.Page[example.com/other.Item]" || actual.Descriptor == nil || actual.Descriptor.Key() != descriptor.Key() {
		t.Fatalf("ResolveShape() = %+v", actual)
	}
	actual.Descriptor.Name = "Mutated"
	again, err := resolver.ResolveShape("[]*model.Page[other.Item]")
	if err != nil {
		t.Fatalf("ResolveShape() second call error = %v", err)
	}
	if again == nil || again.Descriptor == nil || again.Descriptor.Key() != descriptor.Key() {
		t.Fatalf("ResolveShape() leaked caller mutation: %+v", again)
	}
}

func TestGroupPackageComponentSourcesPreservesRouteOrder(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", testGoMod)
	source := strings.Replace(descriptorComponentSource,
		"\tContract xdatly.Component[UsersInput, UsersOutput] `component:\"Users,path=/v1/users,method=GET,view=Users\"`",
		"\tGet  xdatly.Component[UsersInput, UsersOutput] `component:\"Users,path=/v1/users,method=GET,view=Users\" mcp:\"[{\\\"kind\\\":\\\"tool\\\",\\\"name\\\":\\\"users.list\\\"}]\"`\n"+
			"\tPost xdatly.Component[UsersInput, UsersOutput] `component:\"Users,path=/v1/users,method=POST,view=Users\" mcp:\"[{\\\"kind\\\":\\\"tool\\\",\\\"name\\\":\\\"users.create\\\"}]\"`", 1)
	writeFile(t, root, "svc/users/component.go", source)

	routes, err := DiscoverComponentsFromPackages(context.Background(), root, []string{"example.com/app/svc/users"}, nil)
	if err != nil || len(routes) != 2 {
		t.Fatalf("DiscoverComponentsFromPackages() = %+v, %v", routes, err)
	}
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "svc/users")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{
		DefaultPackage: "example.com/app/svc/users", PackageName: "users", PackagePath: "example.com/app/svc/users",
	})
	if err != nil {
		t.Fatal(err)
	}
	components, err := GroupPackageComponentSources(routes, resolver)
	if err != nil || len(components) != 1 || len(components[0].Routes) != 2 {
		t.Fatalf("GroupPackageComponentSources() = %+v, %v", components, err)
	}
	component, err := components[0].ResolveDescriptors(resolver)
	if err != nil {
		t.Fatalf("ResolveDescriptors() error = %v", err)
	}
	if len(component.Routes) != 2 || component.Routes[0].Method != "GET" || component.Routes[1].Method != "POST" {
		t.Fatalf("routes = %+v", component.Routes)
	}
	if len(component.Routes[0].MCP) != 1 || component.Routes[0].MCP[0].Kind != spec.MCPExposureTool || component.Routes[0].MCP[0].Name != "users.list" ||
		len(component.Routes[1].MCP) != 1 || component.Routes[1].MCP[0].Name != "users.create" {
		t.Fatalf("route exposures = %+v", component.Routes)
	}
}

func TestPackageComponentSourceResolveDescriptorsUsesSourceAuthority(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", testGoMod)
	writeFile(t, root, "svc/users/component.go", descriptorComponentSource)

	sources, err := DiscoverComponentsFromPackages(context.Background(), root, []string{"example.com/app/svc/users"}, nil)
	if err != nil || len(sources) != 1 {
		t.Fatalf("DiscoverComponentsFromPackages() = %+v, %v", sources, err)
	}
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "svc/users")
	if err != nil {
		t.Fatalf("LoadPackageFS() error = %v", err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatalf("RegisterPackage() error = %v", err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{
		DefaultPackage: "example.com/app/svc/users",
		PackageName:    "users",
		PackagePath:    "example.com/app/svc/users",
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}

	components, err := GroupPackageComponentSources(sources, resolver)
	if err != nil || len(components) != 1 {
		t.Fatalf("GroupPackageComponentSources() = %+v, %v", components, err)
	}
	component, err := components[0].ResolveDescriptors(resolver)
	if err != nil {
		t.Fatalf("ResolveDescriptors() error = %v", err)
	}
	if component.Name != "Users" || component.Settings == nil || component.Settings.InputType != "UsersInput" ||
		component.Settings.OutputType != "UsersOutput" || len(component.Routes) != 1 || component.Routes[0].Path != "/v1/users" {
		t.Fatalf("component identity = %+v", component)
	}
	if len(component.Parameters) != 3 || component.Parameters[0].Name != "Tenant" || component.Parameters[0].TypeExpr != "string" ||
		component.Parameters[1].Name != "Audit" || component.Parameters[1].TypeExpr != "[]*AuditRow" ||
		component.Parameters[2].Name != "Data" || component.Parameters[2].TypeExpr != "[]*UserRow" {
		t.Fatalf("component params = %+v", component.Parameters)
	}
	if len(component.Views) != 1 || component.Views[0].Name != "Audit" || component.Views[0].Source == nil || component.Views[0].Source.Table != "audit" {
		t.Fatalf("independent views = %+v", component.Views)
	}
	if component.RootView == nil || component.RootView.Name != "Users" || component.RootView.Source == nil ||
		component.RootView.Source.Table != "users" || component.RootView.Source.SQL != "SELECT ID FROM users" {
		t.Fatalf("root view = %+v", component.RootView)
	}
}
