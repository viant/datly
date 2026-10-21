package transcribe

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSourceExplicitScalarPairFreshRuntime(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	fields := `Tenant int ` + "`parameter:\"TenantID,kind=query,in=tenant_id,dataType=*int\"`" + `
 Name Name ` + "`parameter:\",kind=query,in=name,dataType=*" + handlerFixtureModule + "/business.Name\"`" + `
 Count Count ` + "`parameter:\",kind=form,in=count,dataType=*" + handlerFixtureModule + "/business.Count\"`" + `
 Active *bool ` + "`parameter:\",kind=query,in=active,dataType=bool\"`"
	code := strings.Replace(string(data), "Tenant    int      `parameter:\"TenantID,kind=query,in=tenant_id\"`", fields, 1)
	require.NotEqual(t, string(data), code)
	code += `\n` // replaced below to keep fixture source literal simple
	code = strings.TrimSuffix(code, `\n`) + "\ntype Name string\nfunc (n Name) String() string { return string(n) }\ntype NameAlias = Name\ntype Count int\nfunc (n Count) Int() int { return int(n) }\n"
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	source.Text = strings.Replace(source.Text, "$TenantID<int>(query/tenant_id)", "$TenantID<*int,int>(query/tenant_id)", 1)
	source.Text += "\n#import('business','" + handlerFixtureModule + "/business')\n#set($_ = $Name<*business.NameAlias,business.Name>(query/name).Optional())\n#set($_ = $Count<*business.Count,business.Count>(form/count).Optional())\n#set($_ = $Active<bool,*bool>(query/active).Optional())"
	for i := 0; i < 2; i++ {
		compiled, err := NewCompiler().Compile(context.Background(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
	}
	main := fmt.Sprintf(`package main
import("fmt";"reflect"; _ %q;"github.com/viant/datly/bootstrap";compiler "github.com/viant/datly/runtime/handler/compiler";"github.com/viant/datly/spec";"github.com/viant/datly/typecatalog")
func main(){
 packages,err:=bootstrap.ReflectPackages([]string{%q,%q});if err!=nil{panic(err)}
 holder:=packages.Components[0];component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType);if err!=nil{panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil);if err!=nil{panic(err)}
 wants:=map[string]string{"TenantID":"int","Name":"%s/business.Name","Count":"%s/business.Count","Active":"*bool"}
 for _,p:=range component.Parameters {if want,ok:=wants[p.Name];ok&&p.OutputTypeExpr!=want{panic(fmt.Sprintf("destination %%s: %%q want %%q",p.Name,p.OutputTypeExpr,want))}}
 for _,describe:=range []bool{false,true}{c:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type});var result *compiler.Result;if describe{result,err=c.Describe()}else{result,err=c.Compile()};if err!=nil{panic(err)};route,ok:=result.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/convert"});if !ok{panic("route")};seen:=0;for _,field:=range route.Fields(){name:=field.Path();if name=="Tenant"{name="TenantID"};if _,ok:=wants[name];ok{seen++;schema:=field.WireSchema();if schema==nil{panic(field.Path()+" missing schema")};if schema.Nullable!=(field.Path()!="Active"){panic("nullability")};if field.SourceType()!=field.DestinationType(){panic("conversion")};if field.Path()=="Name"&&field.DestinationType().Kind()!=reflect.String{panic("named leaf")}}};if seen!=4{panic(fmt.Sprintf("seen %%d",seen))}}
 fmt.Print("retained")
}`, handlerFixtureModule+"/registration", handlerFixtureModule+"/registration", handlerFixtureModule+"/business", handlerFixtureModule, handlerFixtureModule)
	writeSourceHandlerFile(t, root, "probe/main.go", main)
	cmd := exec.Command("go", "run", "./probe")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "retained", strings.TrimSpace(string(out)))
}

type linkedScalarName string
type linkedScalarOther string

func TestLinkedExplicitScalarPairs(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[linkedScalarName](), reflect.TypeFor[linkedScalarOther]()} {
		require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)))
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	named := reflect.TypeFor[linkedScalarName]().PkgPath() + ".linkedScalarName"
	other := reflect.TypeFor[linkedScalarOther]().PkgPath() + ".linkedScalarOther"
	for _, tc := range []struct {
		name, wire, persisted, output string
		dest                          reflect.Type
		valid                         bool
	}{
		{"int", "*int", "*int", "int", reflect.TypeFor[int](), true},
		{"inverse bool", "bool", "bool", "*bool", reflect.TypeFor[*bool](), true},
		{"named", "*" + named, "*" + named, named, reflect.TypeFor[linkedScalarName](), true},
		{"different named", "*" + other, "*" + other, named, reflect.TypeFor[linkedScalarName](), false},
		{"unbacked", "*int", "int", "int", reflect.TypeFor[int](), false},
		{"wrong width", "*int32", "*int32", "int64", reflect.TypeFor[int64](), false},
		{"bad destination", "*int", "*int", "int32", reflect.TypeFor[int](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: tc.dest, Tag: reflect.StructTag(`parameter:",kind=query,in=value,dataType=` + tc.persisted + `"`)}})
			authored := &spec.Parameter{Name: "Value", TypeExpr: tc.wire, OutputTypeExpr: tc.output, Source: spec.BindSource{Kind: "query", Name: "value"}}
			actual := &spec.Parameter{Name: "Value", TypeExpr: tc.persisted, Source: authored.Source}
			err := validateHandlerParameters(&HandlerBinding{input: typ}, []*spec.Parameter{authored}, []*spec.Parameter{actual}, resolver)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
