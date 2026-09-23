package generate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

type fingerprintFixture struct {
	dir              string
	files, userFiles []EmittedFile
}

func (f *fingerprintFixture) init(t *testing.T) {
	t.Helper()
	f.dir = t.TempDir()
	f.files = []EmittedFile{
		{Path: filepath.Join(f.dir, "handler.go"), Content: "package records\nfunc Handle() string {return \"generated\"}\n"},
		{Path: filepath.Join(f.dir, "handlers", "main.velty"), Content: `#set($Output.Value = "generated")`},
		{Path: filepath.Join(f.dir, "router.go"), Content: "package records\nconst Route = \"old\"\n"},
	}
	f.userFiles = []EmittedFile{{Path: filepath.Join(f.dir, "hooks.go"), Content: "package records\nfunc Hook(){}\n"}}
	if err := f.persistence().Commit(); err != nil {
		t.Fatal(err)
	}
}
func (f *fingerprintFixture) persistence() *scaffoldPersistence {
	return &scaffoldPersistence{dir: f.dir, owner: "Records", files: append([]EmittedFile(nil), f.files...), userFiles: f.userFiles}
}

func TestGeneratedFileFingerprintsProtectEdits(t *testing.T) {
	for _, test := range []struct {
		name, file                               string
		edit, stale, change, sameAsDesired, fail bool
	}{
		{name: "unchanged regeneration"},
		{name: "untouched generated update", file: "handler.go", change: true},
		{name: "edited Go conflict", file: "handler.go", edit: true, change: true, fail: true},
		{name: "edited Velty conflict", file: "handlers/main.velty", edit: true, change: true, fail: true},
		{name: "edited Go no-op conflict", file: "handler.go", edit: true, fail: true},
		{name: "stale untouched deletion", file: "handler.go", stale: true},
		{name: "stale edited Go deletion", file: "handler.go", edit: true, stale: true, fail: true},
		{name: "stale edited Velty deletion", file: "handlers/main.velty", edit: true, stale: true, fail: true},
		{name: "identical desired bytes safe", file: "handler.go", edit: true, change: true, sameAsDesired: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &fingerprintFixture{}
			fixture.init(t)
			path := filepath.Join(fixture.dir, filepath.FromSlash(test.file))
			if test.edit {
				content := "manual edit\n"
				if test.sameAsDesired {
					content = "new generated\n"
				}
				if err := os.WriteFile(path, []byte(content), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(fixture.userFiles[0].Path, []byte("package records\nfunc CustomHook(){}\n"), 0644); err != nil {
				t.Fatal(err)
			}
			p := fixture.persistence()
			var next []EmittedFile
			for _, file := range p.files {
				if file.Path == path {
					if test.stale {
						continue
					}
					if test.change {
						file.Content = "new generated\n"
					}
				}
				if test.fail && filepath.Base(file.Path) == "router.go" {
					file.Content = "package records\nconst Route=\"new\"\n"
				}
				next = append(next, file)
			}
			p.files = next
			before, err := readScaffoldSnapshot(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			validateErr := p.Validate()
			commitErr := p.Commit()
			if test.fail {
				if validateErr == nil || commitErr == nil || !strings.Contains(commitErr.Error(), "manually changed") {
					t.Fatalf("validate=%v commit=%v", validateErr, commitErr)
				}
				after, err := readScaffoldSnapshot(fixture.dir)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("conflict partially published: %v", err)
				}
				return
			}
			if validateErr != nil || commitErr != nil {
				t.Fatalf("validate=%v commit=%v", validateErr, commitErr)
			}
			if test.stale {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("untouched stale file remains: %v", err)
				}
			}
			manifest, err := readScaffoldManifest(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range p.files {
				relative, _ := managedPath(fixture.dir, file.Path)
				if manifest.Fingerprints[relative] != scaffoldFingerprint([]byte(file.Content)) {
					t.Fatalf("missing generated fingerprint for %s", relative)
				}
			}
			if _, found := manifest.Fingerprints["hooks.go"]; found {
				t.Fatal("user hook acquired generated ownership")
			}
			hooks, err := os.ReadFile(fixture.userFiles[0].Path)
			if err != nil || string(hooks) != "package records\nfunc CustomHook(){}\n" {
				t.Fatalf("hooks changed: %q %v", hooks, err)
			}
		})
	}
}

func TestOverwritePolicyReplacesTrustedMalformedGeneratedShape(t *testing.T) {
	dir := t.TempDir()
	malformed := "package records\n\n// Input is generated.\ntype Input struct {\n\tKeywordFrom *time.Time `parameter:\"keyword_from,kind=form\"`\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "input.go"), []byte(malformed), 0o644); err != nil {
		t.Fatal(err)
	}
	metadata := &scaffoldManifest{
		Roles:        map[string]string{"input.go": "shape"},
		Fingerprints: map[string]string{"input.go": scaffoldFingerprint([]byte(malformed))},
	}
	if err := writeScaffoldManifest(dir, "Records", []string{"input.go"}, metadata); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		ComponentName: "Records",
		RouterDest:    "router.go",
		Input:         generatedContract("Input", "input.go"),
		Output:        ContractPlan{Ownership: ContractLinked, Type: "Output"},
		Imports:       []spec.ImportSpec{{Alias: "time", Package: "time"}},
	}
	plan.Input.Fields = []Field{{Name: "KeywordFrom", Type: "*time.Time", Tag: `parameter:"keyword_from,kind=form"`}}
	if _, err := EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), "time") {
		t.Fatalf("merge mode did not expose malformed existing shape: %v", err)
	}
	if _, err := EmitScaffoldWithPolicy(dir, plan, GenerationPolicyOverwrite); err != nil {
		t.Fatalf("overwrite regeneration failed: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "input.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), `time "time"`) || !strings.Contains(string(content), "*time.Time") {
		t.Fatalf("input.go was not replaced with corrected generated source:\n%s", content)
	}
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Fingerprints["input.go"] != scaffoldFingerprint(content) {
		t.Fatalf("manifest fingerprint not refreshed: %+v", manifest.Fingerprints)
	}
	ownership := manifest.ProjectionFields["input.go"]
	if ownership == nil || !ownership.Complete {
		t.Fatalf("projection ownership not refreshed: %+v", ownership)
	}
	foundKeywordFrom := false
	for _, field := range ownership.Fields {
		foundKeywordFrom = foundKeywordFrom || field.Owner == "Input" && field.Name == "KeywordFrom"
	}
	if !foundKeywordFrom {
		t.Fatalf("projection ownership missing KeywordFrom: %+v", ownership)
	}
}

func TestOverwritePolicyRejectsEditedGeneratedShape(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: ContractPlan{Ownership: ContractLinked, Type: "Output"}}
	plan.Input.Fields = []Field{{Name: "ID", Type: "int"}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "input.go"), []byte("package records\n\ntype Input struct { ID string }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan.Input.Fields = []Field{{Name: "ID", Type: "int"}, {Name: "Name", Type: "string"}}
	if _, err := EmitScaffoldWithPolicy(dir, plan, GenerationPolicyOverwrite); err == nil || !strings.Contains(err.Error(), "manually changed") {
		t.Fatalf("overwrite accepted edited generated shape: %v", err)
	}
}

func TestOverwritePolicyRemovesObsoleteOwnedShape(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	plan.Output = ContractPlan{Ownership: ContractLinked, Type: "Output"}
	if _, err := EmitScaffoldWithPolicy(dir, plan, GenerationPolicyOverwrite); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "output.go")); !os.IsNotExist(err) {
		t.Fatalf("obsolete output.go remains: %v", err)
	}
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range manifest.Files {
		if file == "output.go" {
			t.Fatalf("obsolete output.go remains in manifest: %+v", manifest.Files)
		}
	}
}

func TestGeneratedFingerprintOldManifestMigration(t *testing.T) {
	for _, mode := range []string{"identical", "changed", "stale"} {
		t.Run(mode, func(t *testing.T) {
			fixture := &fingerprintFixture{}
			fixture.init(t)
			manifest, err := readScaffoldManifest(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			manifest.Version = 2
			manifest.Fingerprints = nil
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(fixture.dir, scaffoldManifestName), data, 0644); err != nil {
				t.Fatal(err)
			}
			p := fixture.persistence()
			if mode == "changed" {
				p.files[0].Content = "changed"
			}
			if mode == "stale" {
				p.files = p.files[1:]
			}
			before, err := readScaffoldSnapshot(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			err = p.Commit()
			if mode != "identical" {
				if err == nil || !strings.Contains(err.Error(), "no trusted fingerprint") {
					t.Fatalf("migration error=%v", err)
				}
				after, err := readScaffoldSnapshot(fixture.dir)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("migration conflict changed package")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			manifest, err = readScaffoldManifest(fixture.dir)
			if err != nil || manifest.Version != scaffoldManifestVersion || len(manifest.Fingerprints) != len(fixture.files) {
				t.Fatalf("safe migration manifest=%+v err=%v", manifest, err)
			}
		})
	}
}

func TestStagingEditGuardPreservesLatestUserFiles(t *testing.T) {
	for _, file := range []string{"handler.go", "handlers/main.velty", "hooks.go", "new_notes.txt"} {
		t.Run(file, func(t *testing.T) {
			fixture := &fingerprintFixture{}
			fixture.init(t)
			p := fixture.persistence()
			stage := t.TempDir()
			if err := p.copyExisting(fixture.dir, stage); err != nil {
				t.Fatal(err)
			}
			original, err := readScaffoldSnapshot(stage)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(fixture.dir, filepath.FromSlash(file))
			if err := os.WriteFile(path, []byte("edit during staging"), 0644); err != nil {
				t.Fatal(err)
			}
			before, err := readScaffoldSnapshot(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.swap(fixture.dir, stage, original); err == nil || !strings.Contains(err.Error(), "changed during staging") {
				t.Fatalf("swap error=%v", err)
			}
			after, err := readScaffoldSnapshot(fixture.dir)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("latest edit lost during publication")
			}
		})
	}
}

func TestGeneratorProtectsEditedGoAndVeltyHandlers(t *testing.T) {
	for _, target := range []string{"go", "velty"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			input := Input{Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders"}
			if target == "go" {
				input.GoHandler = parseHandlerAsset(t, "HandleOrders", validHandlerSource("HandleOrders"))
			} else {
				input.Component = veltyHandlerComponent()
				input.VeltyHandler = &VeltyHandlerAsset{Template: `#set($Output.Result = $Input.Name)`}
			}
			result, err := New(input).Generate(dir)
			if err != nil {
				t.Fatal(err)
			}
			file := result.Plan.GoHandler
			path := ""
			if target == "go" {
				path = file.Destination
			} else {
				path = result.Plan.VeltyHandler.ResourceDestination
			}
			absolute := filepath.Join(dir, path)
			original, err := os.ReadFile(absolute)
			if err != nil {
				t.Fatal(err)
			}
			edited := append(append([]byte(nil), original...), []byte("\n// manual extension\n")...)
			if target == "velty" {
				edited = append(append([]byte(nil), original...), []byte("\n## manual extension\n")...)
			}
			if err := os.WriteFile(absolute, edited, 0644); err != nil {
				t.Fatal(err)
			}
			before, err := readScaffoldSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := New(input).Generate(dir); err == nil || !strings.Contains(err.Error(), "manually changed") {
				t.Fatalf("handler conflict=%v", err)
			}
			after, err := readScaffoldSnapshot(dir)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("edited handler package changed")
			}
		})
	}
}

func TestRetainedSQLResourceDoesNotAcquireEditedBaseline(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Resources: &ResourcePlan{Namespace: "queries", Destination: "resources.go", Files: []EmittedFile{{Path: "old.sql", Content: "SELECT 1"}, {Path: "current.sql", Content: "SELECT 2"}}}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "old.sql"), []byte("SELECT 42"), 0644); err != nil {
		t.Fatal(err)
	}
	plan.Resources.Files = plan.Resources.Files[1:]
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Fingerprints["old.sql"] != scaffoldFingerprint([]byte("SELECT 1")) {
		t.Fatal("retained edit was adopted as generator-owned bytes")
	}
	plan.Resources.Files = append(plan.Resources.Files, EmittedFile{Path: "old.sql", Content: "SELECT 3"})
	if _, err := EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), "manually changed") {
		t.Fatalf("retained edit conflict=%v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "old.sql"))
	if err != nil || string(content) != "SELECT 42" {
		t.Fatal("retained edited resource changed")
	}
}

func TestOverwritePrunesOnlyUneditedObsoleteSQLResources(t *testing.T) {
	for _, edited := range []bool{false, true} {
		t.Run(fmt.Sprintf("edited=%t", edited), func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Resources: &ResourcePlan{Namespace: "queries", Destination: "resources.go", Files: []EmittedFile{{Path: "old.sql", Content: "SELECT 1"}, {Path: "current.sql", Content: "SELECT 2"}}}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			if edited {
				if err := os.WriteFile(filepath.Join(dir, "old.sql"), []byte("SELECT 42"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			plan.Resources.Files = plan.Resources.Files[1:]
			_, err := EmitScaffoldWithPolicy(dir, plan, GenerationPolicyOverwrite)
			if edited {
				if err == nil || !strings.Contains(err.Error(), "manually changed") {
					t.Fatalf("edited obsolete resource removal = %v", err)
				}
				if _, statErr := os.Stat(filepath.Join(dir, "old.sql")); statErr != nil {
					t.Fatalf("edited obsolete resource was removed: %v", statErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, statErr := os.Stat(filepath.Join(dir, "old.sql")); !os.IsNotExist(statErr) {
				t.Fatalf("obsolete resource still exists: %v", statErr)
			}
			manifest, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Resources == nil || len(manifest.Resources.Files) != 1 || manifest.Resources.Files[0] != "current.sql" {
				t.Fatalf("resource manifest retained obsolete SQL: %+v", manifest.Resources)
			}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatalf("merge after prune: %v", err)
			}
		})
	}
}

func TestCustomizedShapeBaselineSurvivesAppendAndLinkedTransition(t *testing.T) {
	for _, oldManifest := range []bool{false, true} {
		for _, appendField := range []bool{false, true} {
			t.Run(fmt.Sprintf("old=%v/append=%v", oldManifest, appendField), func(t *testing.T) {
				dir := t.TempDir()
				plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
				plan.Input.Fields = []Field{{Name: "ID", Type: "int"}}
				if _, err := EmitScaffold(dir, plan); err != nil {
					t.Fatal(err)
				}
				manifest, err := readScaffoldManifest(dir)
				if err != nil {
					t.Fatal(err)
				}
				baseline := manifest.Fingerprints["input.go"]
				if oldManifest {
					manifest.Version = 2
					manifest.Fingerprints = nil
					data, err := json.Marshal(manifest)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, scaffoldManifestName), data, 0644); err != nil {
						t.Fatal(err)
					}
					baseline = ""
				}
				input, err := os.ReadFile(filepath.Join(dir, "input.go"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "input.go"), append(input, []byte("\n// authored helper\nfunc (i *Input) Custom() int {return i.ID}\n")...), 0644); err != nil {
					t.Fatal(err)
				}
				if appendField {
					plan.Input.Fields = append(plan.Input.Fields, Field{Name: "Name", Type: "string"})
				}
				files, userFiles, removals, err := scaffoldArtifacts(dir, plan)
				if err != nil {
					t.Fatal(err)
				}
				p := &scaffoldPersistence{dir: dir, owner: plan.ComponentName, files: files, userFiles: userFiles, removals: removals, plan: plan}
				if oldManifest && appendField {
					if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "input_setters.go") || !strings.Contains(err.Error(), "no trusted fingerprint") {
						t.Fatalf("old manifest changed generated setters without migration: %v", err)
					}
					return
				}
				if err := p.Validate(); err != nil {
					t.Fatal(err)
				}
				if err := p.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := p.Commit(); err != nil {
					t.Fatalf("retry: %v", err)
				}
				manifest, err = readScaffoldManifest(dir)
				if err != nil || manifest.Fingerprints["input.go"] != baseline {
					t.Fatalf("authored shape baseline was adopted: %+v %v", manifest, err)
				}
				before, err := readScaffoldSnapshot(dir)
				if err != nil {
					t.Fatal(err)
				}
				plan.Input = ContractPlan{Ownership: ContractLinked, Destination: "input.go", Type: "contracts.Input"}
				if _, err := EmitScaffold(dir, plan); err == nil {
					t.Fatal("linked transition deleted authored shape")
				}
				after, err := readScaffoldSnapshot(dir)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("linked transition partially published")
				}
			})
		}
	}
}
