package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func sourceHandlerAdapterFixture(t *testing.T) (string, *Source) {
	t.Helper()
	root, source := sourceHandlerFixture(t)
	source.Text = strings.Replace(source.Text, `"Factory":`, `"AdapterFactory":"`+handlerFixtureModule+`/adapter.LinkedHandler", "Factory":`, 1)
	writeSourceHandlerFile(t, root, "adapter/adapter.go", handlerAdapterFixture)
	return root, source
}

func TestSourceHandlerAdapterGenerationRuntime(t *testing.T) {
	root, source := sourceHandlerAdapterFixture(t)
	before := sourceHandlerSnapshot(t, root)
	compiled, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, before, sourceHandlerSnapshot(t, root), "compilation must not publish files")
	require.Equal(t, handlerFixtureModule+"/business.NewConvert", compiled.Component.Routes[0].Handler)
	require.Equal(t, handlerFixtureModule+"/adapter", compiled.ExternalHandler.AdapterPackage)
	generator := Generator{Operation: "handler"}
	_, err = generator.Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	require.NoError(t, err)
	first := sourceHandlerSnapshot(t, root)
	_, err = generator.Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	require.Equal(t, first, sourceHandlerSnapshot(t, root), "regeneration must be byte-for-byte identical")
	for _, file := range []string{"router.go", "links.go"} {
		data, err := os.ReadFile(filepath.Join(root, "registration", file))
		require.NoError(t, err)
		require.Contains(t, string(data), ".LinkedHandler")
		require.NotContains(t, string(data), ".Factory[")
	}
	code, err := os.ReadFile("testdata/handleronly/adapter_runtime_test.go.txt")
	require.NoError(t, err)
	writeSourceHandlerFile(t, root, "registration/adapter_runtime_test.go", string(code))
	command := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-count=1", "-timeout=2m", "./registration")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

func TestSourceHandlerAdapterFailuresDoNotPublish(t *testing.T) {
	root, original := sourceHandlerAdapterFixture(t)
	generator := Generator{Operation: "handler"}
	invalid := *original
	invalid.Text = strings.Replace(invalid.Text, "/adapter.LinkedHandler", "/adapter.Absent", 1)
	before := sourceHandlerSnapshot(t, root)
	_, err := generator.Generate(t.Context(), GenerationRequest{Source: &invalid, Destination: root})
	require.ErrorContains(t, err, "not found")
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
	_, err = generator.Generate(t.Context(), GenerationRequest{Source: original, Destination: root})
	require.NoError(t, err)
	for _, tc := range []struct{ factory, message string }{
		{"Absent", "not found"},
		{"hidden", "exported"},
		{"NotFunction", "declared function"},
		{"WithoutError", "exact signature"},
		{"WithArgument", "exact signature"},
		{"Variadic", "exact signature"},
		{"Generic", "exact signature"},
		{"WrongError", "exact signature"},
		{"Lookalike", "exact signature"},
		{"Concrete", "exact signature"},
	} {
		t.Run(tc.factory, func(t *testing.T) {
			source := *original
			source.Text = strings.Replace(source.Text, "/adapter.LinkedHandler", "/adapter."+tc.factory, 1)
			before := sourceHandlerSnapshot(t, root)
			_, err := generator.Generate(t.Context(), GenerationRequest{Source: &source, Destination: root})
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func TestDeclarativeHandlerAdapterAliases(t *testing.T) {
	root, source := sourceHandlerAdapterFixture(t)
	source.Text = `#package('` + handlerFixtureModule + `/registration')
#import('business', '` + handlerFixtureModule + `/business')
#import('bridge', '` + handlerFixtureModule + `/adapter')
#setting($_ = $handler_factory('business.NewConvert', 'Convert'))
#setting($_ = $handler_adapter('bridge.AliasHandler'))
#setting($_ = $route('/convert', 'POST'))
#setting($_ = $input_type('business.Alias'))
#setting($_ = $output_type('business.Output'))
#setting($_ = $case_format('lc'))`
	result, err := (Generator{Operation: "handler"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	require.Equal(t, handlerFixtureModule+"/business.NewConvert", result.Result.Plan.Handler)
	require.Equal(t, "lc", result.Result.Plan.Settings.CaseFormat)
	require.Contains(t, result.Result.Plan.AdapterExpression, ".AliasHandler")
}

const handlerAdapterFixture = `package adapter

import (
    "context"
    "errors"
    "os"
    "reflect"
    "sync/atomic"

    business "github.com/viant/datly/handlerfixture/business"
    rhandler "github.com/viant/datly/runtime/handler"
    "github.com/viant/datly/runtime/handler/custom"
)

var Constructions atomic.Int32
var Recovered atomic.Int32
var Mode string
var ConstructorError = errors.New("adapter construction failed")
var ExecutionError = errors.New("adapter execution failed")

type RuntimeHandler = rhandler.TypedHandler
type RuntimeError = error

func LinkedHandler() (rhandler.TypedHandler, error) {
    if os.Getenv("DATLY_HANDLER_GENERATING") == "1" { panic("generation invoked adapter") }
    Constructions.Add(1)
    switch Mode {
    case "constructor error": return nil, ConstructorError
    case "nil": return nil, nil
    case "typed nil": return (*probe)(nil), nil
    case "mismatch": return &probe{}, nil
    }
    return custom.NewFunc(execOutput), nil
}

func execOutput(_ context.Context, in *business.Input) (out *business.Output, err error) {
    defer func() { if recover() != nil { Recovered.Add(1) } }()
    switch in.Inclusion {
    case "panic": panic("authored panic recovery")
    case "nil": return nil, nil
    case "error": return nil, ExecutionError
    }
    return &business.Output{Value: "adapter:" + in.Inclusion}, nil
}

func AliasHandler() (RuntimeHandler, RuntimeError) { return LinkedHandler() }
func WithoutError() rhandler.TypedHandler { panic("must not run") }
func WithArgument(int) (rhandler.TypedHandler, error) { panic("must not run") }
func Variadic(...int) (rhandler.TypedHandler, error) { panic("must not run") }
func Generic[T any]() (rhandler.TypedHandler, error) { panic("must not run") }
func WrongError() (rhandler.TypedHandler, *struct{}) { panic("must not run") }
type OtherHandler interface {
    InputType() reflect.Type
    OutputType() reflect.Type
    Execute(context.Context, rhandler.Invocation) (any, error)
}
func Lookalike() (OtherHandler, error) { panic("must not run") }
func Concrete() (*probe, error) { panic("must not run") }
var NotFunction = LinkedHandler
type probe struct{}
func (*probe) InputType() reflect.Type { return reflect.TypeFor[struct{}]() }
func (*probe) OutputType() reflect.Type { return reflect.TypeFor[struct{}]() }
func (*probe) Execute(context.Context, rhandler.Invocation) (any, error) { panic("invalid adapter executed") }
`
