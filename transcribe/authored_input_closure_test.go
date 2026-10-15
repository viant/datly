package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/transcribe/gobuild"
)

func TestAuthoredInputClosureRejectsStaleNestedCatalog(t *testing.T) {
	for _, change := range []string{"child field", "child tag", "child marker"} {
		t.Run(change, func(t *testing.T) {
			root, result := authoredReadTestResult(t, false)
			file := filepath.Join(root, "rows/readrow.go")
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			source := string(raw)
			switch change {
			case "child field":
				source = strings.Replace(source, "type ReadChild struct {", "type ReadChild struct {\n Added string `sqlx:\"ADDED\"`", 1)
			case "child tag":
				source = strings.Replace(source, "`sqlx:\"PARENT_ID\"`", "`sqlx:\"CHANGED_PARENT_ID\"`", 1)
			case "child marker":
				source = strings.Replace(source, "type ReadChildHas struct{ ID, ParentID bool }", "type ReadChildHas struct{ ID, ParentID, Added bool }", 1)
			}
			if source == string(raw) {
				t.Fatal("nested mutation setup failed")
			}
			if err = os.WriteFile(file, []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			refs, proof, err := authoredInputViewReferences(context.Background(), result)
			if err != nil {
				t.Fatal(err)
			}
			if len(refs) != 0 || proof != nil {
				t.Fatal("stale nested catalog admitted by fresh physical snapshot")
			}
		})
	}
}

func TestAuthoredInputClosurePreservesHelperKindsAndRecursion(t *testing.T) {
	for _, kind := range []string{"scalar alias", "recursive"} {
		t.Run(kind, func(t *testing.T) {
			root, _ := authoredReadTestResult(t, false)
			file := filepath.Join(root, "rows/readrow.go")
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			source := string(raw)
			if kind == "scalar alias" {
				source = strings.Replace(source, "ID       int", "ID       LocalID", 1) + "\ntype BaseID int\ntype LocalID = BaseID\n"
			} else {
				source = strings.Replace(source, "type ReadChild struct {", "type ReadChild struct {\n Previous *ReadRow `sqlx:\"-\"`", 1)
			}
			if err = os.WriteFile(file, []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/ordinaryread/dql"}, SourceFile: "probe.dql", GoBuild: &gobuild.Context{Dir: root}}).Compile(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			refs, proof, err := authoredInputViewReferences(context.Background(), project.Components[0])
			if err != nil || len(refs) != 1 || proof == nil {
				t.Fatalf("genuine unchanged local closure rejected: refs=%d proof=%t error=%v", len(refs), proof != nil, err)
			}
			if err = proof.validate(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAuthoredInputPersistentModKeepsModuleMetadata(t *testing.T) {
	root, result := authoredReadTestResult(t, false)
	dep := filepath.Join(root, "dependency")
	if err := os.MkdirAll(dep, 0700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"go.mod": "module example.com/dependency\ngo 1.25.0\n", "value.go": "package dependency\n"} {
		if err := os.WriteFile(filepath.Join(dep, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	mod := filepath.Join(root, "go.mod")
	raw, err := os.ReadFile(mod)
	if err != nil {
		t.Fatal(err)
	}
	before := append(raw, []byte("\nreplace example.com/dependency => ./dependency\n")...)
	if err = os.WriteFile(mod, before, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "go.sum"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "rows/dependency.go"), []byte("package rows\nimport _ \"example.com/dependency\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := filepath.Join(root, "private-goenv")
	if err = os.WriteFile(env, []byte("GOFLAGS=-mod=mod\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOFLAGS", "")
	result.Source.GoBuild.Env = append(result.Source.GoBuild.Env, "GOENV="+env)
	selected, overlay, err := authoredInputBuild(context.Background(), result.Source.GoBuild)
	if err != nil || overlay {
		t.Fatalf("private persistent flags failed: %v", err)
	}
	_, flags, err := authoredInputFlags(context.Background(), selected)
	if err != nil || len(flags) != 2 || flags[0] != "-mod=mod" || flags[1] != "-mod=readonly" {
		t.Fatalf("effective readonly normalization failed: %v %v", flags, err)
	}
	_, _, err = authoredInputViewReferences(context.Background(), result)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"go.mod": string(before), "go.sum": ""} {
		after, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != want {
			t.Fatalf("ordinary readonly selection modified %s", name)
		}
	}
}

func TestAuthoredInputBuildPreservesQuotedFlagsAndModes(t *testing.T) {
	root, _ := authoredReadTestResult(t, false)
	for _, flags := range []string{"-mod=mod '-ldflags=-X main.Name=a b'", "-mod=vendor", "-mod=readonly", "-mod=mod -mod=readonly", "'-modfile=a b.mod' -mod=mod"} {
		t.Run(flags, func(t *testing.T) {
			build := &gobuild.Context{Dir: root, Tags: "selected", Env: []string{"GOFLAGS=" + flags}}
			selected, overlay, err := authoredInputBuild(context.Background(), build)
			if err != nil || overlay {
				t.Fatal(err)
			}
			raw, _, err := authoredInputFlags(context.Background(), selected)
			if err != nil {
				t.Fatal(err)
			}
			want := flags
			if strings.HasSuffix(flags, "-mod=mod") || strings.HasPrefix(flags, "-mod=mod '") {
				want += " -mod=readonly"
			}
			if strings.TrimSpace(raw) != want || selected.Tags != build.Tags {
				t.Fatalf("flag or tag authority changed: %q want %q", raw, want)
			}
			if len(build.Env) != 1 || build.Env[0] != "GOFLAGS="+flags {
				t.Fatal("original build context mutated")
			}
		})
	}
}
