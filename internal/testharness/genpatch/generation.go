package genpatch

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ObserveHooks edits only the generator's create-once application hook file.
func ObserveHooks(t testing.TB, directory string) string {
	t.Helper()
	path := filepath.Join(directory, "lifecycle.go")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(content), "return nil", `if state.Original!=nil && state.Original.Has("Start") && !state.Original.Has("End") { if entity.End.IsZero(){panic("invariant backfill missing before Init")};hookObservedOriginal=true };return nil`, 1) + "\nvar hookObservedOriginal bool\n"
	if err = os.WriteFile(path, []byte(edited), 0644); err != nil {
		t.Fatal(err)
	}
	return edited
}

func Run(t testing.TB, root, directory, source string, options ...string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, "runtime_test.go"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	args := append([]string{"test", "-mod=mod", "-count=1"}, options...)
	args = append(args, "./...")
	command := exec.Command("go", args...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated Go runtime: %v\n%s", err, output)
	}
	if len(options) > 0 {
		t.Log(string(output))
	}
}
