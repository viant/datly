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
		code := generationCommand(context.Background(), []string{"transcribe", "handler", "-dir", root, "-connector", "runtimeOnly", "-generation-policy", "overwrite", "example.com/generated/source"}, &out, &diagnostic, binding)
		require.Equal(t, 0, code, diagnostic.String())
		router, err := os.ReadFile(filepath.Join(root, "registration", "router.go"))
		require.NoError(t, err)
		require.Contains(t, string(router), "connector=runtimeOnly")
	}
	require.Equal(t, before, fixture.Constructions.Load())
}
