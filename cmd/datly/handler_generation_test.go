package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe"
	fixture "github.com/viant/datly/transcribe/testdata/handleronly"
)

func TestHandlerTranscriptionCommand(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	require.NoError(t, os.Mkdir(filepath.Join(root, "source"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source", "convert.dql"), []byte(`/* {"URI":"/convert","Method":"POST","Name":"Convert","Type":"legacy.Handler","InputType":"legacy.Input","OutputType":"legacy.Output","MCPTool":true} */`), 0600))
	binding, err := transcribe.NewHandlerBinding[fixture.Input, fixture.Output](transcribe.HandlerMapping{LegacyType: "legacy.Handler", LegacyInput: "legacy.Input", LegacyOutput: "legacy.Output", FactoryPackage: "github.com/viant/datly/transcribe/testdata/handleronly", FactoryName: "NewConvert", DestinationPackage: "example.com/generated/registration"}, fixture.NewConvert)
	require.NoError(t, err)
	before := fixture.Constructions.Load()
	for range 2 {
		var out, diagnostic bytes.Buffer
		code := generationCommand(context.Background(), []string{"transcribe", "handler", "-dir", root, "-connector", "runtimeOnly", "example.com/generated/source"}, &out, &diagnostic, binding)
		require.Equal(t, 0, code, diagnostic.String())
		router, err := os.ReadFile(filepath.Join(root, "registration", "router.go"))
		require.NoError(t, err)
		require.Contains(t, string(router), "connector=runtimeOnly")
		_, err = os.Stat(filepath.Join(root, "registration", ".datly-gen.json"))
		require.True(t, os.IsNotExist(err), "manifest persisted: %v", err)
	}
	require.Equal(t, before, fixture.Constructions.Load())
}

func TestSourceHandlerTranscriptionCommand(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	require.NoError(t, os.Mkdir(filepath.Join(root, "business"), 0700))
	require.NoError(t, os.Mkdir(filepath.Join(root, "source"), 0700))
	code := `package business
import (
    "context"
    "github.com/viant/xdatly/handler"
)
type Input struct {}
type Output struct { Value string }
type Handler struct {}
func New() handler.Contract[Input, Output] { panic("generation must not construct handlers") }
func (*Handler) Exec(context.Context, handler.Session, *Input, *Output) error { return nil }
func init() { panic("generation must not execute initialization") }
`
	require.NoError(t, os.WriteFile(filepath.Join(root, "business", "handler.go"), []byte(code), 0600))
	dql := `/* {"URI":"/convert","Method":"POST","Name":"Convert",
"Factory":"example.com/generated/business.New","InputType":"example.com/generated/business.Input","OutputType":"example.com/generated/business.Output"} */
#package('example.com/generated/registration')`
	require.NoError(t, os.WriteFile(filepath.Join(root, "source", "convert.dql"), []byte(dql), 0600))
	for range 2 {
		var out, diagnostic bytes.Buffer
		status := generationCommand(context.Background(), []string{"transcribe", "handler", "-dir", root, "example.com/generated/source"}, &out, &diagnostic)
		require.Equal(t, 0, status, diagnostic.String())
		router, err := os.ReadFile(filepath.Join(root, "registration", "router.go"))
		require.NoError(t, err)
		require.Contains(t, string(router), "handler=example.com/generated/business.New")
		require.NotContains(t, string(router), "connector=")
		_, err = os.Stat(filepath.Join(root, "registration", ".datly-gen.json"))
		require.True(t, os.IsNotExist(err), "manifest persisted: %v", err)
	}
	var out, diagnostic bytes.Buffer
	status := generationCommand(context.Background(), []string{"transcribe", "handler", "-dir", root, "-schema", "-driver", "must-not-open", "example.com/generated/source"}, &out, &diagnostic)
	require.Equal(t, 2, status)
	require.Contains(t, diagnostic.String(), "not database column discovery")
}
