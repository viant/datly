package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestProjectionOwnershipMigrationAndAtomicRemoval(t *testing.T) {
	for _, tc := range []struct {
		name              string
		version           int
		custom, editField bool
		failure           string
	}{
		{name: "current ownership preserves customization", custom: true},
		{name: "trusted version 3 bootstrap", version: 3},
		{name: "customized version 3 fails closed", version: 3, custom: true, failure: "trustworthy generated field ownership"},
		{name: "version 2 without fingerprints fails closed", version: 2, failure: "trustworthy generated field ownership"},
		{name: "edited generated field fails closed", custom: true, editField: true, failure: "customized type or tag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ID", Type: "int"}, {Name: "Extra", Type: "int"}}}}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			shapePath := filepath.Join(dir, "records.go")
			manifest, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			baseline := manifest.Fingerprints["records.go"]
			if tc.version != 0 {
				manifest.Version = tc.version
				manifest.ProjectionFields = nil
				if tc.version == 2 {
					manifest.Fingerprints = nil
				}
				encoded, err := json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(dir, scaffoldManifestName), encoded, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if tc.custom {
				source, err := os.ReadFile(shapePath)
				if err != nil {
					t.Fatal(err)
				}
				text := strings.Replace(string(source), "Extra int", "Extra int\n Authored string", 1)
				if tc.editField {
					text = strings.Replace(text, "Extra int", "Extra string", 1)
				}
				text += "\n// application-owned\nfunc(r Row) Note() string{return r.Authored}\n"
				if err = os.WriteFile(shapePath, []byte(text), 0644); err != nil {
					t.Fatal(err)
				}
			}
			plan.Views[0].Fields = plan.Views[0].Fields[:1]
			plan.Description = "new router description"
			before, err := readScaffoldSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = plan.ValidateDestination(dir); (err != nil) != (tc.failure != "") {
				t.Fatalf("preflight error %v", err)
			}
			_, err = EmitScaffold(dir, plan)
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("commit error %v", err)
				}
				after, err := readScaffoldSnapshot(dir)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("failed removal partially published: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			source, err := os.ReadFile(shapePath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(source), "Extra int") {
				t.Fatalf("obsolete field retained: %s", source)
			}
			if tc.custom && (!strings.Contains(string(source), "Authored string") || !strings.Contains(string(source), "application-owned")) {
				t.Fatalf("authored content removed: %s", source)
			}
			afterManifest, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if afterManifest.Version != scaffoldManifestVersion || !afterManifest.ProjectionFields["records.go"].Complete {
				t.Fatal("ownership migration incomplete")
			}
			if tc.custom && afterManifest.Fingerprints["records.go"] != baseline {
				t.Fatal("customized source fingerprint blessed")
			}
			if _, err = EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			again, err := os.ReadFile(shapePath)
			if err != nil || string(again) != string(source) {
				t.Fatal("removal not idempotent", err)
			}
		})
	}
}

func TestProjectionOwnershipDoesNotAdoptPreexistingAuthoredField(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ID", Type: "int"}}}}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	shapePath := filepath.Join(dir, "records.go")
	source, err := os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	source = []byte(strings.Replace(string(source), "ID int", "ID int\n Authored int", 1))
	if err = os.WriteFile(shapePath, source, 0644); err != nil {
		t.Fatal(err)
	}
	plan.Views[0].Fields = append(plan.Views[0].Fields, Field{Name: "Authored", Type: "int"})
	if _, err = EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	plan.Views[0].Fields = plan.Views[0].Fields[:1]
	if _, err = EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(shapePath)
	if err != nil || !strings.Contains(string(after), "Authored int") {
		t.Fatal("preexisting field was silently adopted and deleted", err)
	}
}

func TestProjectionCodecAuthorityProtectsUnrelatedTags(t *testing.T) {
	for _, tc := range []struct {
		next    string
		allowed bool
	}{
		{`parameter:"Derived,kind=param" codec:"structql,new"`, true},
		{`parameter:"Changed,kind=param" codec:"structql,new"`, false},
		{`parameter:"Derived,kind=param" codec:"structql,new" custom:"new"`, false},
	} {
		field := projectionField{Tag: `parameter:"Derived,kind=param" codec:"structql,old"`}
		allowed, err := field.codecChange(projectionField{Tag: tc.next})
		if err != nil || allowed != tc.allowed {
			t.Fatalf("codec authority for %q: %v %v", tc.next, allowed, err)
		}
	}
}

func TestProjectionOwnershipBootstrapsByteIdenticalVersionTwo(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ID", Type: "int"}, {Name: "Extra", Type: "int"}}}}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Version = 2
	manifest.Fingerprints = nil
	manifest.ProjectionFields = nil
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, scaffoldManifestName), encoded, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	plan.Views[0].Fields = plan.Views[0].Fields[:1]
	if _, err = EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "records.go"))
	if err != nil || strings.Contains(string(content), "Extra int") {
		t.Fatal("byte-identical proposal did not establish field evidence", err)
	}
}
