package generate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/transcribe/gobuild"
	loaderast "github.com/viant/x/loader/ast"
)

// This fixture exercises the actual persistence merge and Go compiler without
// schema discovery or runtime services.
func TestEntityAccessorBuildSelection(t *testing.T) {
	for _, tc := range []struct{ name, filename, source string }{
		{"excluded tag", "authored.go", "//go:build excluded\n\npackage records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n"},
		{"excluded platform", "authored_windows.go", "package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			write := func(name, src string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("go.mod", "module example.com/records\ngo 1.25.0\n")
			write("record.go", "package records\ntype Record struct{Label *string}\nfunc consume(r *Record,v *string){r.SetLabel(v)}\n")
			write(tc.filename, tc.source)
			build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "GOOS=linux", "CGO_ENABLED=0"}}
			plan := &Plan{Package: "example.com/records", accessorBuild: build, EntitySupport: &EntitySupportPlan{Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
			p := &scaffoldPersistence{dir: root, owner: "fixture", plan: plan, renames: map[string]bool{}, files: []EmittedFile{{Path: filepath.Join(root, "entities.go"), Content: generatedHeader("fixture") + "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n"}}}
			if err := p.preserveEntityMethods(root, root); err != nil {
				t.Fatal(err)
			}
			files := map[string][]byte{filepath.Join(root, "entities.go"): []byte(p.files[0].Content)}
			_, err := build.ValidateFiles(plan.Package, files)
			if err != nil {
				t.Fatalf("excluded authored setter must leave generated accessor available to consumer: %v\ngenerated: %s", err, p.files[0].Content)
			}
			if !strings.Contains(p.files[0].Content, "SetLabel") {
				t.Fatal("excluded source claimed generated accessor")
			}
			if plan.accessorValidationNeeded {
				t.Fatal("excluded source requested convenience accessor validation")
			}
		})
	}
}

func TestEntityAccessorBuildSelectionPublication(t *testing.T) {
	root := t.TempDir()
	const pkg = "example.com/records"
	write := func(name, src string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module "+pkg+"\ngo 1.25.0\n")
	write("consume.go", "package records\nfunc consume(r *Record,v *string){r.SetLabel(v)}\n")
	write("authored.go", "//go:build excluded\n\npackage records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n")
	entity, err := parser.ParseFile(token.NewFileSet(), "entities.go", "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "GOOS=linux", "CGO_ENABLED=0"}}
	plan := &Plan{ProjectRoot: root, Package: pkg, GoPackage: "records", ComponentName: "Records", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", accessorBuild: build,
		Views:         []ViewPlan{{Identity: "root", Name: "Record", Type: "Record", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Label", Type: "*string"}}}},
		EntitySupport: &EntitySupportPlan{File: entity, Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
	if _, err = EmitScaffold(root, plan); err != nil {
		t.Fatal("public generation:", err)
	}
	if err = build.Validate(pkg, nil); err != nil {
		t.Fatal("published consumer:", err)
	}
}

func TestEntityAccessorBuildSelectionControls(t *testing.T) {
	for _, tc := range []struct {
		name, filename, prefix, method, caller, tags, flags, mergeError, buildError string
		extra                                                                       string
		wantGenerated, convenience                                                  bool
	}{
		{name: "selected tag", filename: "authored.go", prefix: "//go:build selected\n\n", method: "func(r *Record)SetLabel(v string){r.Label=&v}", caller: "string", tags: "selected", convenience: true},
		{name: "inherited tags", filename: "authored.go", prefix: "//go:build selected\n\n", method: "func(r *Record)SetLabel(v string){r.Label=&v}", caller: "string", flags: "-tags=selected", convenience: true},
		{name: "selected platform", filename: "authored_linux.go", method: "func(r *Record)SetLabel(v string){r.Label=&v}", caller: "string", convenience: true},
		{name: "exact signature", filename: "authored.go", method: "func(r *Record)SetLabel(v *string){if v!=nil{r.Label=v}}", caller: "*string"},
		{name: "selected arity", filename: "authored.go", method: "func(r *Record)SetLabel(v string,n int){}", caller: "*string", mergeError: "SetLabel"},
		{name: "selected invalid body", filename: "authored.go", method: "func(r *Record)SetLabel(v string){r.Missing=&v}", caller: "string", convenience: true, buildError: "Missing"},
		{name: "selected incompatible consumer", filename: "authored.go", method: "func(r *Record)SetLabel(v string){r.Label=&v}", caller: "*string", convenience: true, buildError: "cannot use"},
		{name: "selected duplicate", filename: "authored.go", method: "func(r *Record)SetLabel(v string){r.Label=&v}", caller: "string", extra: "package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n", mergeError: "SetLabel"},
		{name: "excluded arity", filename: "authored.go", prefix: "//go:build excluded\n\n", method: "func(r *Record)SetLabel(v string,n int){}", caller: "*string", wantGenerated: true},
		{name: "excluded syntax", filename: "authored.go", prefix: "//go:build excluded\n\n", method: "not valid Go", caller: "*string", wantGenerated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			write := func(name, src string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("go.mod", "module example.com/records\ngo 1.25.0\n")
			write("record.go", "package records\ntype Record struct{Label *string}\nfunc consume(r *Record,v "+tc.caller+"){r.SetLabel(v)}\n")
			authored := tc.prefix + "package records\n" + tc.method + "\n"
			write(tc.filename, authored)
			if tc.extra != "" {
				write("other.go", tc.extra)
			}
			build := &gobuild.Context{Dir: root, Tags: tc.tags, Env: []string{"GOWORK=off", "GOFLAGS=" + tc.flags, "GOOS=linux", "CGO_ENABLED=0"}}
			plan := &Plan{Package: "example.com/records", accessorBuild: build, EntitySupport: &EntitySupportPlan{Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
			p := &scaffoldPersistence{dir: root, owner: "fixture", plan: plan, renames: map[string]bool{}, files: []EmittedFile{{Path: filepath.Join(root, "entities.go"), Content: generatedHeader("fixture") + "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n"}}}
			err := p.preserveEntityMethods(root, root)
			if tc.mergeError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.mergeError) {
					t.Fatalf("merge error=%v; want %s", err, tc.mergeError)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(p.files[0].Content, "SetLabel") != tc.wantGenerated {
					t.Fatalf("generated accessor=%s", p.files[0].Content)
				}
				if plan.accessorValidationNeeded != tc.convenience {
					t.Fatalf("convenience=%t; want %t", plan.accessorValidationNeeded, tc.convenience)
				}
				_, err = build.ValidateFiles(plan.Package, map[string][]byte{filepath.Join(root, "entities.go"): []byte(p.files[0].Content)})
				if tc.buildError != "" {
					if err == nil || !strings.Contains(err.Error(), tc.buildError) {
						t.Fatalf("build error=%v; want %s", err, tc.buildError)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			current, err := os.ReadFile(filepath.Join(root, tc.filename))
			if err != nil || string(current) != authored {
				t.Fatalf("authored source changed: %v", err)
			}
			if _, err = os.Stat(filepath.Join(root, "entities.go")); !os.IsNotExist(err) {
				t.Fatalf("staged validation published generated source: %v", err)
			}
		})
	}
}

func accessorPublicationFixture(t *testing.T) (string, *Plan, *gobuild.Context) {
	t.Helper()
	root := t.TempDir()
	const pkg = "example.com/records"
	write := func(name, src string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module "+pkg+"\ngo 1.25.0\n")
	write("consume.go", "package records\nfunc consume(r *Record,v *string){r.SetLabel(v)}\n")
	write("authored.go", "//go:build selected\n\npackage records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n")
	entity, err := parser.ParseFile(token.NewFileSet(), "entities.go", "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "GOOS=linux", "CGO_ENABLED=0"}}
	plan := &Plan{ProjectRoot: root, Package: pkg, GoPackage: "records", ComponentName: "Records", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", accessorBuild: build,
		Views:         []ViewPlan{{Identity: "root", Name: "Record", Type: "Record", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Label", Type: "*string"}}}},
		EntitySupport: &EntitySupportPlan{File: entity, Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
	return root, plan, build
}

func TestEntityAccessorBuildSelectionMetadataAndRegeneration(t *testing.T) {
	root, plan, _ := accessorPublicationFixture(t)
	if err := os.WriteFile(filepath.Join(root, "invalid.go"), []byte("//go:build neverselected\n\npackage other\nnot valid Go\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := EmitScaffold(root, plan); err != nil {
		t.Fatal(err)
	}
	if plan.accessorValidationNeeded || !plan.accessorSourceValidationNeeded || len(plan.handlerGoFiles) == 0 {
		t.Fatal("excluded-only decision did not receive final source validation")
	}
	if _, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "."); err == nil {
		t.Fatal("unfiltered metadata control unexpectedly accepted excluded invalid source")
	}
	if _, err := loaderast.LoadPackageFS(context.Background(), plan.PackageFS(root, "."), "."); err != nil {
		t.Fatal("validated metadata reload:", err)
	}
	before, err := readScaffoldSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(root, plan); err != nil {
		t.Fatal("repeat:", err)
	}
	after, err := readScaffoldSnapshot(root)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("repeat changed bytes or modes: %v", err)
	}
	// Changing selection on the same plan must recompute both ownership and metadata.
	plan.accessorBuild.Tags = "selected"
	if err = os.WriteFile(filepath.Join(root, "consume.go"), []byte("package records\nfunc consume(r *Record,v string){r.SetLabel(v)}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(root, plan); err != nil {
		t.Fatal("activate authored:", err)
	}
	if !plan.accessorValidationNeeded {
		t.Fatal("selected convenience claim was not recomputed")
	}
	content, err := os.ReadFile(filepath.Join(root, "entities.go"))
	if err != nil || strings.Contains(string(content), "SetLabel") {
		t.Fatalf("active authored accessor not retained: %v", err)
	}
	plan.accessorBuild.Tags = ""
	if err = os.WriteFile(filepath.Join(root, "consume.go"), []byte("package records\nfunc consume(r *Record,v *string){r.SetLabel(v)}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(root, plan); err != nil {
		t.Fatal("exclude again:", err)
	}
	if plan.accessorValidationNeeded {
		t.Fatal("stale convenience claim leaked across generation")
	}
	content, err = os.ReadFile(filepath.Join(root, "entities.go"))
	if err != nil || !strings.Contains(string(content), "SetLabel") {
		t.Fatalf("generated accessor not restored: %v", err)
	}
	for _, name := range plan.handlerGoFiles {
		if name == "authored.go" || name == "invalid.go" {
			t.Fatalf("stale metadata selection: %v", plan.handlerGoFiles)
		}
	}
}

func TestEntityAccessorBuildSelectionPublicationFailures(t *testing.T) {
	for _, kind := range []string{"arity", "body", "caller", "duplicate", "wrong destination", "cancellation", "no context", "unowned generated destination", "overlay-only selected source"} {
		t.Run(kind, func(t *testing.T) {
			root, plan, build := accessorPublicationFixture(t)
			plan.accessorBuild.Tags = "selected"
			write := func(name, src string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("consume.go", "package records\nfunc consume(r *Record,v string){r.SetLabel(v)}\n")
			want := ""
			switch kind {
			case "arity":
				write("authored.go", "package records\nfunc(r *Record)SetLabel(v string,n int){}\n")
				want = "one value"
			case "body":
				write("authored.go", "package records\nfunc(r *Record)SetLabel(v string){r.Missing=&v}\n")
				want = "Missing"
			case "caller":
				write("consume.go", "package records\nfunc consume(r *Record,v *string){r.SetLabel(v)}\n")
				want = "cannot use"
			case "duplicate":
				write("other.go", "package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n")
				want = "duplicated"
			case "wrong destination":
				other := t.TempDir()
				write("go.work", "go 1.25.0\nuse .\n")
				if err := os.WriteFile(filepath.Join(other, "go.mod"), []byte("module example.com/records\ngo 1.25.0\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(other, "record.go"), []byte("package records\n"), 0600); err != nil {
					t.Fatal(err)
				}
				plan.accessorBuild = &gobuild.Context{Dir: other, Env: build.Env}
				want = "outside selected build package"
			case "cancellation":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				plan.accessorBuild = build.WithContext(ctx)
			case "no context":
				plan.accessorBuild = nil
				want = "differs from"
			case "unowned generated destination":
				write("entities.go", "//go:build neverselected\n\npackage records\n")
				want = "collides with an unowned"
			case "overlay-only selected source":
				write("backing.txt", "package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n")
				data, err := json.Marshal(struct{ Replace map[string]string }{map[string]string{filepath.Join(root, "virtual.go"): filepath.Join(root, "backing.txt")}})
				if err != nil {
					t.Fatal(err)
				}
				write("overlay.json", string(data))
				plan.accessorBuild.Env = append(plan.accessorBuild.Env, "GOFLAGS=-overlay="+filepath.Join(root, "overlay.json"))
				want = "no projected package content"
			}
			before, err := readScaffoldSnapshot(root)
			if err != nil {
				t.Fatal(err)
			}
			_, err = EmitScaffold(root, plan)
			if kind == "cancellation" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation=%v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("generation error=%v; want %s", err, want)
			}
			after, snapshotErr := readScaffoldSnapshot(root)
			if snapshotErr != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("failed publication changed destination: %v", snapshotErr)
			}
			if plan.handlerGoFiles != nil {
				t.Fatal("failed publication advertised validated metadata")
			}
		})
	}
}

func TestEntityAccessorBuildSelectionProjectedSourceAndRemoval(t *testing.T) {
	root := t.TempDir()
	stage := t.TempDir()
	const pkg = "example.com/projected"
	write := func(dir, name, src string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(root, "go.mod", "module "+pkg+"\ngo 1.25.0\n")
	write(stage, "go.mod", "module "+pkg+"\ngo 1.25.0\n")
	write(root, "record.go", "package records\ntype Record struct{Label *string}\nfunc consume(r *Record,v *string){r.SetLabel(v)}\n")
	write(root, "authored.go", "//go:build neverselected\n\npackage records\nfunc(r *Record)SetLabel(v string,n int){}\n")
	write(root, "obsolete.go", generatedHeader("fixture")+"package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n")
	write(stage, "record.go", "package records\ntype Record struct{Label *string}\nfunc consume(r *Record,v string){r.SetLabel(v)}\n")
	write(stage, "authored.go", "package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n")
	build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "CGO_ENABLED=0"}}
	plan := &Plan{Package: pkg, accessorBuild: build, EntitySupport: &EntitySupportPlan{Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
	p := &scaffoldPersistence{dir: root, owner: "fixture", plan: plan, renames: map[string]bool{"obsolete.go": true}, files: []EmittedFile{{Path: filepath.Join(root, "entities.go"), Content: generatedHeader("fixture") + "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n"}}}
	before, err := readScaffoldSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.preserveEntityMethods(root, stage); err != nil {
		t.Fatal("projected ownership:", err)
	}
	if !plan.accessorValidationNeeded || strings.Contains(p.files[0].Content, "SetLabel") {
		t.Fatal("ownership did not use projected authored bytes")
	}
	write(stage, "entities.go", p.files[0].Content)
	if err = p.validateHandlerStage(root, stage); err != nil {
		t.Fatal("projected final compilation:", err)
	}
	for _, name := range plan.handlerGoFiles {
		if name == "obsolete.go" {
			t.Fatal("retired source returned as selected")
		}
	}
	after, err := readScaffoldSnapshot(root)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("private projection touched nominal source: %v", err)
	}
}

func TestEntityAccessorBuildSelectionCreateOnceSource(t *testing.T) {
	root := t.TempDir()
	const pkg = "example.com/createonce"
	write := func(name, src string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module "+pkg+"\ngo 1.25.0\n")
	write("record.go", "package records\ntype Record struct{Label *string}\nfunc consume(r *Record,v string){r.SetLabel(v)}\n")
	const authored = "package records\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n"
	hook, err := parser.ParseFile(token.NewFileSet(), "custom.go", authored, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "CGO_ENABLED=0"}}
	plan := &Plan{Package: pkg, accessorBuild: build, HookScaffold: &HookScaffoldPlan{File: hook, Destination: "custom.go"}, EntitySupport: &EntitySupportPlan{Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
	p := &scaffoldPersistence{dir: root, owner: "fixture", plan: plan, renames: map[string]bool{}, files: []EmittedFile{{Path: filepath.Join(root, "entities.go"), Content: generatedHeader("fixture") + "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n"}}, userFiles: []EmittedFile{{Path: filepath.Join(root, "custom.go"), Content: authored}}}
	if err = p.preserveEntityMethods(root, root); err != nil {
		t.Fatal(err)
	}
	if !plan.accessorValidationNeeded || strings.Contains(p.files[0].Content, "SetLabel") {
		t.Fatal("create-once source did not claim selected accessor")
	}
	if err = build.Validate(pkg, map[string][]byte{filepath.Join(root, "entities.go"): []byte(p.files[0].Content), filepath.Join(root, "custom.go"): []byte(authored)}); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "custom.go")); !os.IsNotExist(err) {
		t.Fatal("selection published create-once source")
	}
}

func TestEntityAccessorBuildSelectionPresenceConsumer(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint("selected=", active), func(t *testing.T) {
			root := t.TempDir()
			write := func(name, src string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("go.mod", "module example.com/presence\ngo 1.25.0\n")
			write("record.go", "package records\ntype Record struct{Label *string;Has *Presence}\ntype Presence struct{Label bool}\n")
			const authored = "//go:build selected\n\npackage records\nfunc(r *Record)SetLabel(v string){r.Label=&v;if r.Has==nil{r.Has=&Presence{}};r.Has.Label=true}\n"
			write("authored.go", authored)
			build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "CGO_ENABLED=0"}}
			if active {
				build.Tags = "selected"
			}
			plan := &Plan{Package: "example.com/presence", accessorBuild: build, EntitySupport: &EntitySupportPlan{Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
			p := &scaffoldPersistence{dir: root, owner: "fixture", plan: plan, renames: map[string]bool{}, files: []EmittedFile{{Path: filepath.Join(root, "entities.go"), Content: generatedHeader("fixture") + "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v;if r.Has==nil{r.Has=&Presence{}};r.Has.Label=true}\n"}}}
			if err := p.preserveEntityMethods(root, root); err != nil {
				t.Fatal(err)
			}
			if err := build.Validate(plan.Package, map[string][]byte{filepath.Join(root, "entities.go"): []byte(p.files[0].Content)}); err != nil {
				t.Fatal(err)
			}
			write("entities.go", p.files[0].Content)
			call := "r.SetLabel(&value)"
			if active {
				call = "r.SetLabel(value)"
			}
			write("consumer_test.go", "package records\nimport \"testing\"\nfunc TestConsumer(t *testing.T){r:=&Record{};value:=\"\";"+call+";if r.Label==nil||*r.Label!=value||r.Has==nil||!r.Has.Label{t.Fatalf(\"lost value or presence: %+v\",r)}}\n")
			args := []string{"test", "-count=1"}
			if active {
				args = append(args, "-tags=selected")
			}
			args = append(args, ".")
			cmd := exec.Command("go", args...)
			cmd.Dir = root
			cmd.Env = append(os.Environ(), build.Env...)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("typed presence consumer: %v: %s", err, output)
			}
			current, err := os.ReadFile(filepath.Join(root, "authored.go"))
			if err != nil || string(current) != authored {
				t.Fatalf("authored setter changed: %v", err)
			}
		})
	}
}

func TestEntityAccessorBuildSelectionSplitBoundary(t *testing.T) {
	p := &Plan{accessorBuild: &gobuild.Context{}, accessorSourceValidationNeeded: true}
	s := &packageSet{plans: []*Plan{p, {}}}
	if err := s.validateAccessorDestinations(p); err == nil || !strings.Contains(err.Error(), "source selection requires a single generated destination") {
		t.Fatalf("source selection boundary=%v", err)
	}
	if p.accessorValidationNeeded {
		t.Fatal("source selection was mislabeled as a convenience claim")
	}
}

func TestEntityAccessorBuildSelectionOrdinarySplitGeneration(t *testing.T) {
	root := t.TempDir()
	const pkg = "example.com/split"
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+pkg+"\ngo 1.25.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	entity, err := parser.ParseFile(token.NewFileSet(), "entities.go", "package models\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=", "CGO_ENABLED=0"}}
	child := &Plan{ProjectRoot: root, Package: pkg + "/api/models", GoPackage: "models", ComponentName: "Records", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", accessorBuild: build,
		Views:         []ViewPlan{{Name: "Record", Type: "Record", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Label", Type: "*string"}}}},
		EntitySupport: &EntitySupportPlan{File: entity, Destination: "entities.go", Methods: []EntityMethod{{Receiver: "Record", Name: "SetLabel", ValueType: "*string", FieldAccessor: true}}}}
	parent := &Plan{ProjectRoot: root, Package: pkg + "/api", GoPackage: "api", ComponentName: "Parent", ShapesOnly: true, RouterDest: "router.go", ViewDest: "shape.go", ShapePackages: []*Plan{child}, Views: []ViewPlan{{Name: "ParentRow", Type: "ParentRow", Destination: "shape.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}}}}}
	target := filepath.Join(root, "api")
	models := filepath.Join(target, "models")
	if _, err = EmitScaffold(target, parent); err != nil {
		t.Fatal("initial generated-only split:", err)
	}
	if child.accessorSourceValidationNeeded || child.accessorValidationNeeded {
		t.Fatal("generated-only source armed authored accessor restriction")
	}
	if _, err = EmitScaffold(target, parent); err != nil {
		t.Fatal("repeat generated-only split:", err)
	}
	if err = os.WriteFile(filepath.Join(models, "custom.go"), []byte("package models\nfunc(r *Record)Other(){}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(target, parent); err != nil {
		t.Fatal("unrelated authored method widened split restriction:", err)
	}
	if child.accessorSourceValidationNeeded || child.accessorValidationNeeded {
		t.Fatal("unrelated authored source armed accessor restriction")
	}
	authored := "//go:build excluded\n\npackage models\nfunc(r *Record)SetLabel(v string){r.Label=&v}\n"
	if err = os.WriteFile(filepath.Join(models, "accessor.go"), []byte(authored), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := readScaffoldSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(target, parent); err == nil || !strings.Contains(err.Error(), "source selection requires a single generated destination") {
		t.Fatalf("excluded authored candidate lost final safety gate: %v", err)
	}
	after, err := readScaffoldSnapshot(root)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected authored split changed files: %v", err)
	}
}

func TestEntityAccessorBuildSelectionMissingProjection(t *testing.T) {
	root, plan, _ := accessorPublicationFixture(t)
	p := &scaffoldPersistence{dir: root, owner: "fixture", plan: plan, renames: map[string]bool{}, files: []EmittedFile{{Path: filepath.Join(root, "entities.go"), Content: generatedHeader("fixture") + "package records\nfunc(r *Record)SetLabel(v *string){r.Label=v}\n"}}}
	if err := p.preserveEntityMethods(root, filepath.Join(root, "missing")); err == nil || !strings.Contains(err.Error(), "projection") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing current projection accepted as generated-only: %v", err)
	}
	if plan.accessorSourceValidationNeeded || plan.handlerGoFiles != nil {
		t.Fatal("missing projection advertised source validation")
	}
}
