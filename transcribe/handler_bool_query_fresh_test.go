package transcribe

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestNativeExplicitBoolQueryWireFreshRuntime(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/querywire"}).Write(t, root)
	source := &Source{Name: "Records", Scope: "example.com/querywire", Path: root, Text: `#package('generated')
#setting($_ = $route('/records','POST'))
#define($_ = $Body<*RecordsView>(body/).Optional())
#define($_ = $Flag<*bool,bool>(query/source).Optional().Value('true'))
SELECT r.id,CAST(r.id AS 'int'),tag(r.id,'sqlx:"id,primaryKey"') FROM records r`}
	generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	var found bool
	for _, f := range generated.Result.Plan.Input.Fields {
		if f.Name == "Flag" {
			found = true
			require.Equal(t, "bool", f.Type)
			require.Contains(t, f.Tag, "dataType=*bool")
		}
	}
	require.True(t, found)
	main := `package main
import("fmt";"reflect";"strings";_ "example.com/querywire/generated";"github.com/viant/datly/bootstrap";compiler "github.com/viant/datly/runtime/handler/compiler";"github.com/viant/datly/transcribe/generate";"github.com/viant/datly/mcp/tool";"github.com/viant/datly/spec";"github.com/viant/datly/typecatalog")
func main(){
 packages,err:=bootstrap.ReflectPackages([]string{"example.com/querywire/generated"});if err!=nil{panic(err)}
 holder:=packages.Components[0];component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType);if err!=nil{panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil);if err!=nil{panic(err)}
 for _,p:=range component.Parameters{if p.Name=="Flag" && (p.TypeExpr!="*bool" || p.OutputTypeExpr!="bool"){panic(fmt.Sprintf("split lost: %+v",p))}}
 component.Settings.InputType="RecordsInput";component.Settings.OutputType="RecordsOutput"
 plan,err:=generate.New(generate.Input{Component:component,TargetPackage:"example.com/querywire/generated",TypeResolver:resolver}).Plan();if err!=nil{panic(err)}
 found:=false;for _,f:=range plan.Input.Fields{if f.Name=="Flag"{found=true;if f.Type!="bool"{panic("regeneration changed destination")};if !strings.Contains(f.Tag,"dataType=*bool"){panic("regeneration lost dataType")}}};if !found{panic("source missing")}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type}).Compile();if err!=nil{panic(err)}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/records"})
 for _,f:=range route.Fields(){if f.Binding().Path=="Flag" && (f.SourceType()!=reflect.TypeFor[bool]() || f.DestinationType()!=reflect.TypeFor[bool]()){panic("binding changed")}}
 discovery,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:&spec.MCPExposure{Kind:spec.MCPExposureTool,Name:"records"}});if err!=nil{panic(err)}
 if !reflect.DeepEqual(discovery.Metadata().InputSchema.Properties["Flag"]["type"],[]string{"boolean","null"}){panic(fmt.Sprintf("schema lost: %+v",discovery.Metadata().InputSchema))}
 fmt.Print("retained")
}`
	writeSourceHandlerFile(t, root, "main.go", main)
	cmd := exec.Command("go", "run", "-mod=readonly", ".")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "retained", strings.TrimSpace(string(out)))
	second, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	for _, f := range second.Result.Plan.Input.Fields {
		if f.Name == "Flag" {
			require.Equal(t, "bool", f.Type)
			require.Contains(t, f.Tag, "dataType=*bool")
		}
	}

}
