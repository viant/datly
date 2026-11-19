package command

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/state"
)

type HandlerImportRequestFixture struct{}
type HandlerImportOptionsFixture struct{}
type HandlerImportExternalFixture struct{}

func TestAddLocalHandlerParameterTypes(t *testing.T) {
	imports := inference.NewImports()
	imports.AddType("advancedreporting/run.Input")
	imports.AddType("advancedreporting/run.Handler")
	imports.AddType("advancedreporting/run.HandlerImportRequestFixture")

	inputState := inference.State{
		handlerInputParameter(reflect.TypeOf(&HandlerImportRequestFixture{}), "github.com/example/project/pkg/advancedreporting/run", false),
		handlerInputParameter(reflect.TypeOf(&HandlerImportOptionsFixture{}), "github.com/example/project/pkg/advancedreporting/run", false),
		handlerInputParameter(reflect.TypeOf(&HandlerImportOptionsFixture{}), "github.com/example/project/pkg/advancedreporting/run", false),
		handlerInputParameter(reflect.TypeOf(&HandlerImportExternalFixture{}), "github.com/example/project/pkg/platform/auth", false),
		handlerInputParameter(reflect.TypeOf(&HandlerImportOptionsFixture{}), "github.com/example/project/pkg/advancedreporting/run", true),
		{Parameter: state.Parameter{Schema: state.NewSchema(reflect.TypeOf(map[string]interface{}{}))}},
		nil,
	}

	addLocalHandlerParameterTypes(&imports, "advancedreporting/run.Input", inputState)

	assert.Equal(t, []string{
		"advancedreporting/run.Input",
		"advancedreporting/run.Handler",
		"advancedreporting/run.HandlerImportRequestFixture",
		"advancedreporting/run.HandlerImportOptionsFixture",
	}, imports.Types)
}

func TestAddLocalHandlerParameterTypesIgnoresUnqualifiedInput(t *testing.T) {
	imports := inference.NewImports()
	inputState := inference.State{
		handlerInputParameter(reflect.TypeOf(&HandlerImportOptionsFixture{}), "github.com/example/project/pkg/advancedreporting/run", false),
	}

	addLocalHandlerParameterTypes(&imports, "Input", inputState)

	assert.Empty(t, imports.Types)
}

func handlerInputParameter(rType reflect.Type, packagePath string, codecOutput bool) *inference.Parameter {
	parameter := &inference.Parameter{Parameter: state.Parameter{
		Schema: state.NewSchema(rType, state.WithPackagePath(packagePath)),
	}}
	if codecOutput {
		parameter.Output = &state.Codec{}
	}
	return parameter
}
