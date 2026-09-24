package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestExternalHandlerHolderImportCollisions(t *testing.T) {
	root := t.TempDir()
	const module = "github.com/viant/datly/holderimports"
	const factory = "github.com/viant/datly/transcribe/testdata/handleronly"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	for _, alias := range []string{"reflect", "xdatly", "rhandler", "customhandler", "name", "registry"} {
		plan := &Plan{
			ComponentName: "Convert", Package: module + "/" + alias,
			Handler: factory + ".NewConvert", FactoryExpression: alias + ".NewConvert",
			ExternalHandler: &ExternalHandler{Package: factory, Name: "NewConvert"},
			Input:           ContractPlan{Type: alias + ".Input", Ownership: ContractLinked},
			Output:          ContractPlan{Type: alias + ".Output", Ownership: ContractLinked},
			Imports:         []spec.ImportSpec{{Alias: alias, Package: factory}},
			Routes:          []RoutePlan{{Path: "/convert", Method: "POST"}},
		}
		source, err := componentFileText("registration", plan)
		require.NoError(t, err)
		dir := filepath.Join(root, alias)
		require.NoError(t, os.Mkdir(dir, 0700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "router.go"), []byte(source), 0600))
		link := &FactoryLinkPlan{Name: "RegisterConvertFactories", PackagePath: factory, Factory: "NewConvert", Adapter: "custom", Expression: plan.FactoryExpression}
		source, err = link.source("registration", plan)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "links.go"), []byte(source), 0600))
	}
	command := exec.Command("go", "test", "-mod=mod", "-timeout=1m", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
