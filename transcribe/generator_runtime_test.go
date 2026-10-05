package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The generated contract and standalone entry point must bind the declared
// generator before application Init, without application provider registration.
func TestGeneratedFactoryGeneratorBeforeInitHTTP(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	source.Text += "\n#define($_ = $Clock<time.Time>(generator/current_time))\n"
	file := filepath.Join(root, "archive/business.go")
	code, err := os.ReadFile(file)
	require.NoError(t, err)
	childSource := *source
	childSource.Name = "Child"
	childSource.Text = strings.ReplaceAll(source.Text, handlerFixtureModule+"/archive", handlerFixtureModule+"/child")
	childSource.Text = strings.Replace(childSource.Text, "'/archive','POST'", "'/archive-child','POST'", 1)
	childCode := strings.Replace(string(code), "package archive", "package child", 1)
	childCode = strings.Replace(childCode, "\"os\"", "\"os\"\n \"time\"\n \"fmt\"", 1)
	childCode += `
func (in *ArchiveInput) Init(context.Context) error {
 if in.Clock.IsZero() || time.Since(in.Clock)<0 || time.Since(in.Clock)>time.Minute { return fmt.Errorf("child clock missing before Init") }
 return nil
}
`
	writeSourceHandlerFile(t, root, "child/business.go", childCode)
	var firstChild map[string]string
	for range 2 {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: childSource.GoBuild}).CompileSource(context.Background(), &childSource)
		require.NoError(t, err)
		_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		childFiles := sourceHandlerSnapshot(t, root)
		if firstChild == nil {
			firstChild = childFiles
		} else {
			require.Equal(t, firstChild, childFiles)
		}
	}
	text := strings.Replace(string(code), "\"os\"", "\"os\"\n \"time\"\n \"fmt\"\n dexec \"github.com/viant/datly/exec\"\n \"github.com/viant/datly/spec\"\n child \"github.com/viant/datly/handlerfixture/child\"", 1)
	text = strings.Replace(text, "Exec(_ context.Context, _ handler.Session", "Exec(ctx context.Context, session handler.Session", 1)
	text = strings.Replace(text, ` out.Status = "ok"`, `
 if in.Data != nil && len(in.Data.Ids)==1 && in.Data.Ids[0]=="nested" {
  value,found,err:=session.Binder().Lookup(ctx,dexec.ComponentInvokerKey)
  if err!=nil{return err};if !found{return fmt.Errorf("scoped child invoker missing")}
  childInput:=&child.ArchiveInput{Data:&child.ArchiveRequest{Ids:[]string{"child"}}}
  result,err:=value.(dexec.ComponentInvoker).InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"github.com/viant/datly/handlerfixture/child",Name:"Archive"},Route:spec.RouteRef{Method:"POST",Path:"/archive-child"}},Input:childInput})
  if err!=nil{return err};if childInput.Clock.IsZero()||childInput.Clock.Before(in.Clock){return fmt.Errorf("child generator clock did not bind")}
  out.Status=result.(*child.ArchiveOutput).Status; for _,item:=range result.(*child.ArchiveOutput).Results {out.Results=append(out.Results,&ResultItem{Id:item.Id,Status:item.Status})}
  return nil
 }
 out.Status = "ok"`, 1)
	text += `
func (in *ArchiveInput) Init(context.Context) error {
 if in.Clock.IsZero() || time.Since(in.Clock) < 0 || time.Since(in.Clock) > time.Minute {
  return fmt.Errorf("generator did not bind before Init: %v", in.Clock)
 }
 return nil
}
`
	require.NoError(t, os.WriteFile(file, []byte(text), 0600))
	var first map[string]string
	var runtimeSource string
	for range 2 {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		runtimeSource = strings.Replace(generatedPostFactoryRuntimeTest, "\n \"testing\"", "\n \"testing\"\n \"time\"\n dexec \"github.com/viant/datly/exec\"\n \"github.com/viant/datly/spec\"\n mcpinvoke \"github.com/viant/datly/mcp/invocation\"\n child \"github.com/viant/datly/handlerfixture/child\"", 1)
		runtimeSource = strings.Replace(runtimeSource, "Holders:[]any{ArchiveDatly}", "Holders:[]any{ArchiveDatly,child.ArchiveDatly}", 1)
		runtimeSource = strings.Replace(runtimeSource, `Packages:[]string{"github.com/viant/datly/handlerfixture/archive"}`, `Packages:[]string{"github.com/viant/datly/handlerfixture/archive","github.com/viant/datly/handlerfixture/child"}`, 1)
		runtimeSource = strings.Replace(runtimeSource, "  cancel()", fmt.Sprintf(`
  supplied := &ArchiveInput{Clock: time.Date(1999,1,1,0,0,0,0,time.UTC)}
  typed, invokeErr := server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:%q,Name:%q},Route:spec.RouteRef{Method:"POST",Path:"/archive"}},Input:supplied})
  require.NoError(t,invokeErr)
  require.Equal(t,"ok",typed.(*ArchiveOutput).Status)
  require.False(t,supplied.Clock.IsZero())
  require.NotEqual(t,1999,supplied.Clock.Year())
  outcome, protocolErr := mcpinvoke.New(mcpinvoke.Config{Invoker:server}).Execute(ctx,mcpinvoke.Request{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:%q,Name:%q},Route:spec.RouteRef{Method:"POST",Path:"/archive"}},Method:"tools/call",URI:"archive.generator"})
  require.Nil(t,protocolErr)
  require.NoError(t,outcome.Error())
  require.Equal(t,"ok",outcome.Value().(*ArchiveOutput).Status)
  nested, nestedErr := server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:%q,Name:%q},Route:spec.RouteRef{Method:"POST",Path:"/archive"}},Input:&ArchiveInput{Data:&ArchiveRequest{Ids:[]string{"nested"}}}})
  require.NoError(t,nestedErr)
  require.Equal(t,"child",nested.(*ArchiveOutput).Results[0].Id)
  cancel()`, handlerFixtureModule+"/archive", compiled.Component.Key.Name, handlerFixtureModule+"/archive", compiled.Component.Key.Name, handlerFixtureModule+"/archive", compiled.Component.Key.Name), 1)
		actual := sourceHandlerSnapshot(t, root)
		if first == nil {
			first = actual
		} else {
			require.Equal(t, first, actual)
		}
	}
	writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtimeSource)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-timeout=2m", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "--- PASS: TestGeneratedPostFactoryRuntime")
}
