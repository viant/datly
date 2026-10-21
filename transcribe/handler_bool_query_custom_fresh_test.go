package transcribe

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceExplicitBoolQueryWireFreshRuntime(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	code := strings.Replace(string(data), "Tenant    int      `parameter:\"TenantID,kind=query,in=tenant_id\"`", "Flag bool `parameter:\",kind=query,in=source,dataType=*bool\" value:\"true\"`", 1)
	require.NotEqual(t, string(data), code)
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	source.Text = strings.Replace(source.Text, "$TenantID<int>(query/tenant_id)", "$Flag<*bool,bool>(query/source)", 1)
	compiled, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		compiled, err = NewCompiler().Compile(context.Background(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
	}
	main := fmt.Sprintf(`package main
import ("fmt"; "reflect"; _ %q; "github.com/viant/datly/bootstrap"; compiler "github.com/viant/datly/runtime/handler/compiler"; "github.com/viant/datly/mcp/tool"; "github.com/viant/datly/spec"; "github.com/viant/datly/typecatalog")
func main(){
 packages,err:=bootstrap.ReflectPackages([]string{%q,%q});if err!=nil{panic(err)}
 holder:=packages.Components[0];component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType);if err!=nil{panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil);if err!=nil{panic(err)}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type}).Compile();if err!=nil{panic(err)}
 for _,p:=range component.Parameters {if p.Name=="Flag" && p.OutputTypeExpr!="bool" {panic("destination lost on reload")}}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/convert"});found:=false
 for _,field:=range route.Fields(){if field.Binding().Path!="Flag"{continue};found=true;if field.SourceType()!=reflect.TypeFor[bool]() || field.DestinationType()!=reflect.TypeFor[bool](){panic("runtime source changed")}}
 if !found{panic("Source lost")}
 plan,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:component.Routes[0].MCP[0]});if err!=nil{panic(err)}
 if !reflect.DeepEqual(plan.Metadata().InputSchema.Properties["Flag"]["type"],[]string{"boolean","null"}){panic(fmt.Sprintf("wire metadata lost: %%#v",plan.Metadata().InputSchema))}
 fmt.Print("retained")
}`, handlerFixtureModule+"/registration", handlerFixtureModule+"/registration", handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "main.go", main)
	cmd := exec.Command("go", "run", "-mod=readonly", ".")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "retained", strings.TrimSpace(string(out)))
}
