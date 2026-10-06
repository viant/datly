package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// An authored helper cannot be retired by another component under the current
// ownership policy. This is an explicitly test-only external projected-edit
// simulation, after genuine packageSet/forest preparation, not such a retirement.
func TestPreparedPristineMethodsExternalProjectedEdit(t *testing.T) {
	root := t.TempDir()
	const module = "example.com/pristinemethods"
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+module+"\n\ngo 1.25.8\n"), 0644); err != nil {
		t.Fatal(err)
	}
	parent, child := filepath.Join(root, "api"), filepath.Join(root, "api", "models")
	if err := os.MkdirAll(child, 0750); err != nil {
		t.Fatal(err)
	}
	const handlerPath = "github.com/viant/xdatly/handler"
	generated := `package models
import handler "github.com/viant/xdatly/handler"
func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields handler.FieldSet) error {
    if prior != nil { e.Id = prior.Id }
    return nil
}`
	entity, err := parser.ParseFile(token.NewFileSet(), "entities.go", generated, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	authored := "package models\nimport renamed \"" + handlerPath + "\"\nfunc (e *Record) BackfillScheduleIfNeeded(prior *Record, fields renamed.FieldSet) error { return nil }\n"
	authoredPath := filepath.Join(child, "record_custom.go")
	if err = os.WriteFile(authoredPath, []byte(authored), 0640); err != nil {
		t.Fatal(err)
	}
	childPlan := &Plan{
		ProjectRoot: root, Package: module + "/api/models", GoPackage: "models", ComponentName: "Records", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go",
		Views:         []ViewPlan{{Name: "Record", Type: "Record", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}},
		EntitySupport: &EntitySupportPlan{File: entity, Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "BackfillScheduleIfNeeded", Signature: "func(*Record, handler.FieldSet) error"}}},
	}
	plan := &Plan{
		ProjectRoot: root, Package: module + "/api", GoPackage: "api", ComponentName: "Parent", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", ShapePackages: []*Plan{childPlan},
		Views: []ViewPlan{{Name: "ParentRow", Type: "ParentRow", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}},
	}
	if _, err = EmitScaffold(parent, plan); err != nil {
		t.Fatal("initial public generation preserving authored helper:", err)
	}
	before, err := readScaffoldSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	packages, err := plan.packageTargets(parent)
	if err != nil {
		t.Fatal(err)
	}
	release, err := packages.lockTargets()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = packages.render(); err != nil {
		t.Fatal(err)
	}
	if err = packages.validate(); err != nil {
		t.Fatal("genuine preview:", err)
	}
	rawBefore := append([]EmittedFile(nil), packages.templates[1].files...)
	previewBefore := append([]EmittedFile(nil), packages.previews[1].files...)
	preparedPristineMethodsCheck(t, preparedPristineMethodsContent(t, packages.templates[1]), 1, true)
	preparedPristineMethodsCheck(t, preparedPristineMethodsContent(t, packages.previews[1]), 0, false)

	// Explicit retirement only removes matching generated ownership. Prove the
	// fixture's unowned authored helper is not a supported removal operation.
	retirement := packages.templates[1].detached()
	retirement.removals = append(retirement.removals, "record_custom.go")
	if err = retirement.prepareCurrentAt(child, child); err != nil {
		t.Fatal(err)
	}
	if retirement.renames["record_custom.go"] {
		t.Fatal("unowned authored helper was admitted for retirement")
	}
	forests, readRoots, _, err := prepareScaffoldForests(packages.templates)
	if err != nil {
		t.Fatal("genuine forest preparation:", err)
	}
	defer cleanupScaffoldForests(forests)
	packages.forests, packages.readRoots = forests, readRoots
	if err = packages.validateProjected(); err != nil {
		t.Fatal("prepared ordinary authored-helper control:", err)
	}
	projectedChild := readRoots[1]
	// Simulate an external earlier edit in the private projection; no owner is
	// assigned to the authored helper and no public authoring claim is made.
	if err = os.Remove(filepath.Join(projectedChild, "record_custom.go")); err != nil {
		t.Fatal(err)
	}
	raw := packages.templates[1].detached()
	stalePreview := packages.previews[1].detached()
	if err = raw.prepareCurrentAt(child, projectedChild); err != nil {
		t.Fatal("raw-template preparation against edited projection:", err)
	}
	if err = stalePreview.prepareCurrentAt(child, projectedChild); err != nil {
		t.Fatal("stale-preview preparation control:", err)
	}
	preparedPristineMethodsCheck(t, preparedPristineMethodsContent(t, raw), 1, true)
	preparedPristineMethodsCheck(t, preparedPristineMethodsContent(t, stalePreview), 0, false)
	if err = raw.writeFiles(child, projectedChild); err != nil {
		t.Fatal("write restored helper to private projection:", err)
	}
	content, err := os.ReadFile(filepath.Join(projectedChild, "entities.go"))
	if err != nil {
		t.Fatal(err)
	}
	preparedPristineMethodsCheck(t, string(content), 1, true)
	if err = packages.validateProjected(); err != nil {
		t.Fatal("final restored projected package/import control:", err)
	}
	if !reflect.DeepEqual(rawBefore, packages.templates[1].files) || !reflect.DeepEqual(previewBefore, packages.previews[1].files) {
		t.Fatal("preparing detached templates mutated retained pristine or preview bytes")
	}
	cleanupScaffoldForests(forests)
	after, err := readScaffoldSnapshot(root)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("external projected-edit witness changed original full paths/modes/bytes: %v", err)
	}
	t.Log("genuine raw template restores method+canonical import; stale preview cannot regenerate either; unowned retirement rejected; physical tree unchanged")
}

func preparedPristineMethodsContent(t *testing.T, p *scaffoldPersistence) string {
	t.Helper()
	for _, file := range p.files {
		if filepath.Base(file.Path) == "entities.go" {
			return file.Content
		}
	}
	t.Fatal("genuine persistence lacks entities.go")
	return ""
}

func preparedPristineMethodsCheck(t *testing.T, source string, wantMethods int, wantImport bool) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "entities.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	methods := 0
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil && fn.Name.Name == "BackfillScheduleIfNeeded" {
			methods++
		}
	}
	imported := false
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if path == "github.com/viant/xdatly/handler" {
			imported = true
		}
	}
	if methods != wantMethods || imported != wantImport {
		t.Fatalf("methods/import = %d/%t, want %d/%t; actual source:\n%s", methods, imported, wantMethods, wantImport, source)
	}
}
