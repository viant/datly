package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/gobuild"
)

// The former probe rewrites this deliberately incomplete, fully local graph.
// No network dependency is needed to prove the readonly failure boundary.
func TestFactoryInputShapeOwnershipReadonlyIncompleteGraph(t *testing.T) {
	for _, tc := range []struct {
		name, conflict string
		cancel         bool
	}{
		{name: "ownership succeeds before staged build"},
		{name: "authored conflict", conflict: "type Res struct{}"},
		{name: "canceled selection", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeSourceHandlerFile(t, root, "go.mod", "module example.com/factory\n\ngo 1.25.8\n\nreplace example.com/dependency => ./dependency\n")
			writeSourceHandlerFile(t, root, "dependency/go.mod", "module example.com/dependency\n\ngo 1.25.8\n")
			writeSourceHandlerFile(t, root, "dependency/value.go", "package dependency\ntype Value int\n")
			writeSourceHandlerFile(t, root, "app/business.go", "package app\nimport _ \"example.com/dependency\"\n"+tc.conflict+"\n")
			t.Setenv("GOFLAGS", "-mod=mod")
			build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GO111MODULE=on", "GOPROXY=off"}}
			before := sourceHandlerSnapshot(t, root)
			ctx := context.Background()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := validateFactoryShapeOwnership(ctx, build, "example.com/factory/app", "Archive", &spec.View{TypeName: "Res", Dest: "res.go"})
			if tc.cancel || tc.conflict != "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				err = build.Validate("example.com/factory/app", nil)
				require.Error(t, err, "the incomplete graph must fail staged validation")
			}
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
			if !tc.cancel {
				unsafe := exec.Command("go", "list", "-e", "-json", "--", "example.com/factory/app")
				unsafe.Dir = root
				unsafe.Env = append(os.Environ(), build.Env...)
				output, controlErr := unsafe.CombinedOutput()
				require.NoError(t, controlErr, "%s", output)
				require.NotEqual(t, before, sourceHandlerSnapshot(t, root), "unprotected control must demonstrate graph mutation")
				mod, readErr := os.ReadFile(filepath.Join(root, "go.mod"))
				require.NoError(t, readErr)
				require.Contains(t, string(mod), "require example.com/dependency")
			}
		})
	}
}

func TestFactoryInputShapeOwnershipBuildSelection(t *testing.T) {
	for _, tc := range []struct{ name, flags, tags, selected string }{
		{"inherited tags", "-tags=ownership", "", "tagged.go"},
		{"explicit tags override", "-tags=other", "ownership", "tagged.go"},
		{"selected modfile readonly", "-mod=mod -modfile=selected.mod", "ownership", "tagged.go"},
		{"vendor preserved", "-mod=vendor", "ownership", "tagged.go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeSourceHandlerFile(t, root, "go.mod", "module example.com/factory\n\ngo 1.25.8\n")
			writeSourceHandlerFile(t, root, "selected.mod", "module example.com/factory\n\ngo 1.25.8\n")
			writeSourceHandlerFile(t, root, "app/base.go", "package app\n")
			writeSourceHandlerFile(t, root, "app/tagged.go", "//go:build ownership\n\npackage app\ntype Res struct{}\n")
			writeSourceHandlerFile(t, root, "app/ignored.go", "//go:build other\n\npackage app\ntype Res struct{}\n")
			before := sourceHandlerSnapshot(t, root)
			build := &gobuild.Context{Dir: root, Tags: tc.tags, Env: []string{"GOWORK=off", "GO111MODULE=on", "GOFLAGS=" + tc.flags}}
			err := validateFactoryShapeOwnership(context.Background(), build, "example.com/factory/app", "Archive", &spec.View{TypeName: "Res", Dest: "res.go"})
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), tc.selected), "%v", err)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}
