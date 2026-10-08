package build

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
)

func linkTestFile(t *testing.T, root, name, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(content), 0644))
}

// Include directories, symlinks and module files: failed validation must not
// leave a seed directory, support file, or dependency-file changes behind.
func linkTree(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			result[relative] = "directory"
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			result[relative] = "symlink:" + target
			return err
		}
		data, err := os.ReadFile(path)
		result[relative] = string(data)
		return err
	}))
	return result
}

func TestSyncLinksFreshDestinations(t *testing.T) {
	for _, tc := range []struct{ option, path string }{
		{"pkg/componentlink", "pkg/componentlink"}, {"", "internal/dependencylink"}, {"legacy", "internal/legacy"}, {"internal/nested/links", "internal/nested/links"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/linkapp"}).Write(t, root)
			writeLinkTestBareComponent(t, root, "reader")
			// The application imports a package whose directory does not exist yet.
			linkTestFile(t, root, "cmd/app/main.go", "package main\nimport _ \"example.com/linkapp/"+tc.path+"\"\nfunc main(){}\n")
			request := LinkRequest{Dir: root, LinkPackage: tc.option}
			result, err := (Service{}).SyncLinks(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, []string{"example.com/linkapp/reader"}, result.Added)
			content, err := os.ReadFile(filepath.Join(root, tc.path, "link.go"))
			require.NoError(t, err)
			require.Contains(t, string(content), "package "+filepath.Base(tc.path))
			require.Contains(t, string(content), `_ "example.com/linkapp/reader"`)
			before := linkTree(t, root)
			result, err = (Service{}).SyncLinks(context.Background(), request)
			require.NoError(t, err)
			require.Empty(t, result.Added)
			require.Equal(t, before, linkTree(t, root))
			command := exec.Command("go", "build", "-mod=readonly", "-o", filepath.Join(t.TempDir(), "app"), "./cmd/app")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off")
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}

func TestSyncLinksExistingPackageAndSelectedImports(t *testing.T) {
	for _, tc := range []struct {
		name, tag, tags string
		wantAdded       bool
	}{
		{"active", "", "", false}, {"excluded", "optional", "", true}, {"enabled", "optional", "optional", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/linkapp"}).Write(t, root)
			writeLinkTestComponent(t, root, "reader", true)
			source := "package wiring\nimport _ \"example.com/linkapp/reader\"\n"
			if tc.tag != "" {
				source = "//go:build " + tc.tag + "\n\n" + source
			}
			linkTestFile(t, root, "pkg/componentlink/authored.go", source)
			result, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/componentlink", Tags: tc.tags})
			require.NoError(t, err)
			require.Equal(t, tc.wantAdded, len(result.Added) > 0)
			content, err := os.ReadFile(filepath.Join(root, "pkg/componentlink/link.go"))
			require.NoError(t, err)
			require.Contains(t, string(content), "package wiring")
			require.Equal(t, tc.wantAdded, strings.Contains(string(content), `"example.com/linkapp/reader"`))
			require.Equal(t, source, linkTree(t, root)["pkg/componentlink/authored.go"])
		})
	}
}

func TestSyncLinksZeroCandidatesCreatesPackage(t *testing.T) {
	root := t.TempDir()
	linkTestFile(t, root, "go.mod", "module example.com/empty\n\ngo 1.25.0\n")
	result, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links"})
	require.NoError(t, err)
	require.Empty(t, result.Added)
	content, err := os.ReadFile(filepath.Join(root, "pkg/links/link.go"))
	require.NoError(t, err)
	require.Equal(t, "package links\n", string(content))
}

func TestSyncLinksPreservesVendorSelection(t *testing.T) {
	for _, flags := range []string{"", "-mod=vendor"} {
		t.Run(flags, func(t *testing.T) {
			root := t.TempDir()
			linkTestFile(t, root, "go.mod", "module example.com/app\n\ngo 1.25.0\nrequire example.com/peer v1.0.0\n")
			linkTestFile(t, root, "vendor/modules.txt", "# example.com/peer v1.0.0\n## explicit; go 1.25.0\nexample.com/peer\n")
			linkTestFile(t, root, "vendor/example.com/peer/peer.go", "package peer\nconst Vendored = true\n")
			linkTestFile(t, root, "pkg/links/authored.go", "package links\nimport \"example.com/peer\"\nconst Value = peer.Vendored\n")
			env := append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOFLAGS="+flags)
			before := linkTree(t, root)
			_, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links", Env: env})
			require.NoError(t, err)
			after := linkTree(t, root)
			delete(after, "pkg/links/link.go")
			require.Equal(t, before, after)
		})
	}
}

func TestSyncLinksRejectsModuleUpdatesWithoutPublishing(t *testing.T) {
	workspace := t.TempDir()
	root := filepath.Join(workspace, "app")
	linkTestFile(t, root, "go.mod", "module example.com/app\n\ngo 1.25.0\nreplace example.com/peer => ../peer\n")
	linkTestFile(t, workspace, "peer/go.mod", "module example.com/peer\n\ngo 1.25.0\n")
	linkTestFile(t, workspace, "peer/peer.go", "package peer\n")
	linkTestFile(t, root, "pkg/links/authored.go", "package links\nimport _ \"example.com/peer\"\n")
	before := linkTree(t, workspace)
	env := append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOFLAGS=-mod=mod")
	_, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links", Env: env})
	require.Error(t, err)
	require.Equal(t, before, linkTree(t, workspace))
}

func TestSyncLinksFailureDoesNotPublish(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
	}{
		{"mixed packages", map[string]string{"pkg/links/a.go": "package first", "pkg/links/b.go": "package second"}},
		{"main package", map[string]string{"pkg/links/a.go": "package main"}},
		{"invalid existing link", map[string]string{"pkg/links/link.go": "package links\nfunc broken("}},
		{"excluded link file", map[string]string{"pkg/links/link.go": "//go:build optional\n\npackage links\n", "pkg/links/active.go": "package links\n"}},
		{"invalid selected source", map[string]string{"broken/broken.go": "package broken\nfunc broken("}},
		{"unowned support", map[string]string{"reader/datly_link_sync.go": "package reader\n"}},
		{"helper collision", map[string]string{"reader/collision.go": "package reader\nvar _anchorComponent = 1\n"}},
		{"import cycle", map[string]string{"reader/cycle.go": "package reader\nimport _ \"example.com/linkapp/pkg/links\"\n"}},
		{"invalid zero-addition package", map[string]string{"pkg/links/link.go": "package links\nimport _ \"example.com/linkapp/reader\"\nvar Broken = missing\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/linkapp"}).Write(t, root)
			writeLinkTestBareComponent(t, root, "reader")
			for name, content := range tc.files {
				linkTestFile(t, root, name, content)
			}
			before := linkTree(t, root)
			_, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links"})
			require.Error(t, err)
			require.Equal(t, before, linkTree(t, root))
		})
	}
}

func TestLinkPackageContainment(t *testing.T) {
	for _, name := range []string{"/tmp/links", "../links", "pkg/../links", "pkg//links", "pkg/links/", "C:\\links", "pkg\\links", "pkg/.hidden", "vendor/links", "internal/main", "internal/_"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			linkTestFile(t, root, "go.mod", "module example.com/app\n\ngo 1.25.0\n")
			before := linkTree(t, root)
			_, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: name})
			require.Error(t, err)
			require.Equal(t, before, linkTree(t, root))
		})
	}
	for _, file := range []string{"pkg", "pkg/links", "pkg/links/link.go"} {
		t.Run(file, func(t *testing.T) {
			root := t.TempDir()
			outside := t.TempDir()
			linkTestFile(t, root, "go.mod", "module example.com/app\n\ngo 1.25.0\n")
			target := outside
			if strings.HasSuffix(file, ".go") {
				target = filepath.Join(outside, "link.go")
				require.NoError(t, os.WriteFile(target, []byte("package links"), 0644))
			}
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, file)), 0755))
			require.NoError(t, os.Symlink(target, filepath.Join(root, file)))
			before, external := linkTree(t, root), linkTree(t, outside)
			_, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links"})
			require.ErrorContains(t, err, "symlink")
			require.ErrorContains(t, (Service{}).Init(context.Background(), InitRequest{Dir: root, LinkPackage: "pkg/links"}), "symlink")
			_, err = (Service{}).Build(context.Background(), Request{Dir: root, LinkPackage: "pkg/links"})
			require.ErrorContains(t, err, "symlink")
			require.Equal(t, before, linkTree(t, root))
			require.Equal(t, external, linkTree(t, outside))
		})
	}
}

func TestInitModuleRelativeLinkPackage(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, root)
	linkTestFile(t, root, "pkg/componentlink/authored.go", "package wiring\nconst Value = 1\n")
	require.NoError(t, (Service{}).Init(context.Background(), InitRequest{Dir: root, LinkPackage: "pkg/componentlink"}))
	files := linkTree(t, root)
	require.Equal(t, "package wiring\n", files["pkg/componentlink/link.go"])
	require.Contains(t, files["cmd/datly/main.go"], `_ "example.com/app/pkg/componentlink"`)
}

func TestSyncLinksPreservesWorkspaceAndDoesNotExecuteCode(t *testing.T) {
	workspace := t.TempDir()
	root := filepath.Join(workspace, "app")
	require.NoError(t, os.MkdirAll(root, 0755))
	(testharness.GeneratedModule{Path: "example.com/linkapp"}).Write(t, root)
	linkTestFile(t, workspace, "go.work", "go 1.25.8\nuse (\n ./app\n ./peer\n)\n")
	linkTestFile(t, workspace, "peer/go.mod", "module example.com/peer\n\ngo 1.25.0\n")
	linkTestFile(t, workspace, "peer/peer.go", "package peer\nconst Value = 1\n")
	writeLinkTestBareComponent(t, root, "reader")
	linkTestFile(t, root, "reader/import.go", "package reader\nimport \"example.com/peer\"\nconst Value = peer.Value\nfunc init(){panic(\"must not execute during sync\")}\n")
	request := LinkRequest{Dir: root, LinkPackage: "pkg/links", Env: append(os.Environ(), "GOWORK="+filepath.Join(workspace, "go.work"))}
	_, err := (Service{}).SyncLinks(context.Background(), request)
	require.NoError(t, err)
	linkTestFile(t, root, "reader/import.go", "package reader\nimport \"example.com/peer\"\nconst Value = peer.Missing\n")
	before := linkTree(t, workspace)
	_, err = (Service{}).SyncLinks(context.Background(), request)
	require.Error(t, err)
	require.Equal(t, before, linkTree(t, workspace))
}

func TestSyncLinksRejectsNestedModule(t *testing.T) {
	root := t.TempDir()
	linkTestFile(t, root, "go.mod", "module example.com/app\n\ngo 1.25.0\n")
	linkTestFile(t, root, "pkg/go.mod", "module example.com/nested\n\ngo 1.25.0\n")
	before := linkTree(t, root)
	_, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links"})
	require.ErrorContains(t, err, "nested module")
	require.Equal(t, before, linkTree(t, root))
}

func TestLinkPlanRejectsConcurrentChangeBeforePublishing(t *testing.T) {
	root := t.TempDir()
	plan, err := newLinkPlan(root)
	require.NoError(t, err)
	defer plan.close()
	path := filepath.Join(root, "pkg/links/link.go")
	file, err := plan.file(path)
	require.NoError(t, err)
	file.updated = []byte("package links\n")
	helper, err := plan.file(filepath.Join(root, "reader/datly_link_sync.go"))
	require.NoError(t, err)
	helper.updated = []byte("package reader\n")
	linkTestFile(t, root, "pkg/links/link.go", "package links\n// concurrent edit\n")
	before := linkTree(t, root)
	require.ErrorContains(t, plan.publish(context.Background(), path), "changed during sync")
	require.Equal(t, before, linkTree(t, root))
}

func TestLinkPlanReportsPartialPublication(t *testing.T) {
	root := t.TempDir()
	linkTestFile(t, root, "pkg/links/link.go", "package links\n")
	plan, err := newLinkPlan(root)
	require.NoError(t, err)
	defer plan.close()
	linkPath := filepath.Join(root, "pkg/links/link.go")
	link, err := plan.file(linkPath)
	require.NoError(t, err)
	link.updated = []byte("package links\n// new imports\n")
	helper, err := plan.file(filepath.Join(root, "reader/datly_link_sync.go"))
	require.NoError(t, err)
	helper.updated = []byte("package reader\nfunc init() {}\n")
	err = plan.publishWith(context.Background(), linkPath, func(dir *os.Root, file *plannedLinkFile) error {
		if file == link {
			return errors.New("simulated write failure")
		}
		return publishLinkFile(dir, file)
	})
	require.ErrorContains(t, err, "already published: [reader/datly_link_sync.go]")
	require.ErrorContains(t, err, "simulated write failure")
	files := linkTree(t, root)
	require.Equal(t, "package links\n", files["pkg/links/link.go"])
	require.Equal(t, string(helper.updated), files["reader/datly_link_sync.go"])
	for path := range files {
		require.NotContains(t, path, ".datly-link-")
	}
}

func TestSyncLinksExcludesSelectedLinkPackage(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/linkapp"}).Write(t, root)
	writeLinkTestBareComponent(t, root, "reader")
	linkTestFile(t, root, "pkg/links/holder.go", "package links\nimport xdatly \"github.com/viant/xdatly\"\ntype Holder struct{Read xdatly.Component[struct{},struct{}] `component:\"Own,path=/own,method=GET\"`}\n")
	result, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root, LinkPackage: "pkg/links"})
	require.NoError(t, err)
	require.Equal(t, []string{"example.com/linkapp/reader"}, result.Added)
	files := linkTree(t, root)
	require.NotContains(t, files["pkg/links/link.go"], `"example.com/linkapp/pkg/links"`)
	_, exists := files["pkg/links/datly_link_sync.go"]
	require.False(t, exists)
}
