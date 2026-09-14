package report

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
)

func TestRuntimeComponentsExecuteCompleteReportProject(t *testing.T) {
	compilation := compiledReportProject(t)
	var sourceInput *reportSourceInput
	registered, err := compilation.RuntimeComponents(context.Background(), RuntimeConfigureFunc(func(
		_ context.Context, artifact *ComponentArtifact,
	) (RuntimeCapabilities, error) {
		if !artifact.IsReport() {
			return RuntimeCapabilities{Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](func(_ context.Context, input *reportSourceInput) (*reportSourceOutput, error) {
				sourceInput = input
				return &reportSourceOutput{Rows: []reportSourceRow{{AccountID: input.AccountIDs[0]}}}, nil
			})}, nil
		}
		return RuntimeCapabilities{}, nil
	}))
	if err != nil {
		t.Fatalf("RuntimeComponents() error = %v", err)
	}
	if len(registered) != 2 || registered[1].Handler == nil {
		t.Fatalf("registered components = %+v", registered)
	}
	runtime, err := druntime.NewRuntime(registered)
	if err != nil {
		t.Fatal(err)
	}
	reportArtifact := compilation.Artifacts()[1]
	input := reflect.New(reportArtifact.InputType())
	input.Elem().FieldByName("Dimensions").FieldByName(typecatalog.ExportedFieldName("AccountID")).SetBool(true)
	accountIDs := []int{11}
	input.Elem().FieldByName("Filters").FieldByName(typecatalog.ExportedFieldName("AccountIDs")).Set(reflect.ValueOf(&accountIDs))
	component := reportArtifact.Component()
	actual, err := runtime.InvokeComponent(context.Background(), exec.ComponentRequest{
		Target: exec.ComponentTarget{
			Component: component.Key,
			Route:     spec.RouteRef{Method: component.Routes[0].Method, Path: component.Routes[0].Path},
		},
		Input: input.Interface(),
	})
	if err != nil {
		t.Fatalf("InvokeComponent() error = %v", err)
	}
	if sourceInput == nil || !reflect.DeepEqual(sourceInput.AccountIDs, accountIDs) {
		t.Fatalf("source input = %+v", sourceInput)
	}
	output, ok := actual.(*reportSourceOutput)
	if !ok || len(output.Rows) != 1 || output.Rows[0].AccountID != 11 {
		t.Fatalf("report output = %#v", actual)
	}
}

func TestRuntimeComponentsExposeReportThroughGenericMCP(t *testing.T) {
	compilation := compiledReportProject(t)
	var sourceInput *reportSourceInput
	registered, err := compilation.RuntimeComponents(context.Background(), RuntimeConfigureFunc(func(
		_ context.Context, artifact *ComponentArtifact,
	) (RuntimeCapabilities, error) {
		if !artifact.IsReport() {
			return RuntimeCapabilities{Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](func(_ context.Context, input *reportSourceInput) (*reportSourceOutput, error) {
				sourceInput = input
				return &reportSourceOutput{Rows: []reportSourceRow{{AccountID: input.AccountIDs[0]}}}, nil
			})}, nil
		}
		return RuntimeCapabilities{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime(registered)
	if err != nil {
		t.Fatal(err)
	}
	service, err := mcp.New(mcp.Config{Components: registered, Invoker: runtime})
	if err != nil {
		t.Fatalf("mcp.New() error = %v", err)
	}
	if names := service.Catalog().ToolNames(); !reflect.DeepEqual(names, []string{"SpendCube"}) {
		t.Fatalf("MCP tools = %v", names)
	}
	tool, ok := service.Registry().ToolRegistry.Get("SpendCube")
	if !ok {
		t.Fatal("derived report tool was not registered")
	}
	result, protocolErr := tool.Handler(context.Background(), &schema.CallToolRequest{
		Method: schema.MethodToolsCall,
		Params: schema.CallToolRequestParams{
			Name: "SpendCube",
			Arguments: map[string]interface{}{
				"dimensions": map[string]interface{}{"accountID": true},
				"filters":    map[string]interface{}{"accountIDs": []interface{}{13}},
			},
		},
	})
	if protocolErr != nil || result == nil || result.IsError != nil && *result.IsError {
		t.Fatalf("MCP report result = %+v, error=%+v", result, protocolErr)
	}
	if sourceInput == nil || !reflect.DeepEqual(sourceInput.AccountIDs, []int{13}) {
		t.Fatalf("MCP source input = %+v", sourceInput)
	}
}

func TestRuntimeComponentsFailWithoutPublishingPartialSet(t *testing.T) {
	compilation := compiledReportProject(t)
	configured := 0
	registered, err := compilation.RuntimeComponents(context.Background(), RuntimeConfigureFunc(func(
		_ context.Context, artifact *ComponentArtifact,
	) (RuntimeCapabilities, error) {
		configured++
		if artifact.IsReport() {
			return RuntimeCapabilities{}, errors.New("report setup failed")
		}
		return RuntimeCapabilities{Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](nil)}, nil
	}))
	if err == nil || !strings.Contains(err.Error(), "report setup failed") || registered != nil || configured != 2 {
		t.Fatalf("RuntimeComponents() = %+v, %v, configured=%d", registered, err, configured)
	}
}

func TestRuntimeComponentsProtectPredefinedReportHandler(t *testing.T) {
	compilation := compiledReportProject(t)
	_, err := compilation.RuntimeComponents(context.Background(), RuntimeConfigureFunc(func(
		_ context.Context, artifact *ComponentArtifact,
	) (RuntimeCapabilities, error) {
		if !artifact.IsReport() {
			return RuntimeCapabilities{Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](nil)}, nil
		}
		return RuntimeCapabilities{Handler: artifact.predefinedHandler}, nil
	}))
	if err == nil || !strings.Contains(err.Error(), "replaced predefined report handler") {
		t.Fatalf("RuntimeComponents() error = %v", err)
	}
}

func TestRuntimeComponentsProtectCompiledMetadataFromConfigurator(t *testing.T) {
	compilation := compiledReportProject(t)
	registered, err := compilation.RuntimeComponents(context.Background(), RuntimeConfigureFunc(func(
		_ context.Context, artifact *ComponentArtifact,
	) (RuntimeCapabilities, error) {
		metadata := artifact.Component()
		metadata.Routes = nil
		metadata.Settings.InputType = "Changed"
		if artifact.IsReport() {
			return RuntimeCapabilities{}, nil
		}
		return RuntimeCapabilities{Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](nil)}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for index, component := range registered {
		if len(component.Component.Routes) != 1 || component.Component.Settings.InputType == "Changed" {
			t.Fatalf("registered component %d was mutated: %+v", index, component.Component)
		}
	}
	for index, artifact := range compilation.Artifacts() {
		component := artifact.Component()
		if len(component.Routes) != 1 || component.Settings.InputType == "Changed" {
			t.Fatalf("compiled component %d was mutated: %+v", index, component)
		}
	}
}

func compiledReportProject(t *testing.T) *Compilation {
	t.Helper()
	compilation, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).CompileArtifacts([]bootstrap.ArtifactInput{
		reportArtifactInput(t, &spec.ReportSettings{Enabled: true}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return compilation
}
