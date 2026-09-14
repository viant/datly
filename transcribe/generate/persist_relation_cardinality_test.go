package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	xshape "github.com/viant/x/shape"
)

func TestRelationCardinalityOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, edit, failure string
		version             int
		partial, unowned    bool
	}{
		{name: "generated"},
		{name: "mixed authored fields", edit: "comment"},
		{name: "trusted old manifest", version: 3},
		{name: "customized old manifest", version: 3, edit: "comment", failure: "trustworthy generated field ownership"},
		{name: "old manifest without fingerprints", version: 2, failure: "trustworthy generated field ownership"},
		{name: "partial known holder", partial: true},
		{name: "partial unknown holder", partial: true, unowned: true, failure: "trustworthy generated field ownership"},
		{name: "edited type", edit: "type", failure: "customized type or tag"},
		{name: "edited type equals proposal", edit: "proposal", failure: "customized type or tag"},
		{name: "edited tag", edit: "tag", failure: "customized type or tag"},
		{name: "changed child identity", edit: "child", failure: "changes relation child type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{
				{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Child", Type: "[]*Child", Tag: `on:"ID:ID"`, RelationHolder: true}}},
				{Name: "Child", Type: "Child", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ID", Type: "int"}}},
			}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "records.go")
			manifest, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if tc.version != 0 {
				manifest.Version = tc.version
				manifest.ProjectionFields = nil
				if tc.version == 2 {
					manifest.Fingerprints = nil
				}
			}
			if tc.partial {
				manifest.ProjectionFields["records.go"].Complete = false
				if tc.unowned {
					manifest.ProjectionFields["records.go"].Fields = manifest.ProjectionFields["records.go"].Fields[1:]
				}
			}
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, scaffoldManifestName), data, 0644); err != nil {
				t.Fatal(err)
			}
			data, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			switch tc.edit {
			case "comment":
				data, err = (xshape.SourceParser{}).AppendStructFields(data, []byte("package records;type Row struct{Authored string}"))
				if err != nil {
					t.Fatal(err)
				}
				text = string(data) + "\n// application-owned\nfunc(r Row) Note()string{return r.Authored}\n"
			case "type":
				text = strings.Replace(text, "[]*Child", "[]Child", 1)
			case "proposal":
				text = strings.Replace(text, "[]*Child", "*Child", 1)
			case "tag":
				text = strings.Replace(text, `on:"ID:ID"`, `on:"ID:Other"`, 1)
			}
			if err = os.WriteFile(path, []byte(text), 0644); err != nil {
				t.Fatal(err)
			}
			plan.Views[0].Fields[0].Type = "*Child"
			if tc.edit == "child" {
				plan.Views[0].Fields[0].Type = "*Row"
			}
			before, err := readScaffoldSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = plan.ValidateDestination(dir); (err != nil) != (tc.failure != "") {
				t.Fatalf("preflight: %v", err)
			}
			_, err = EmitScaffold(dir, plan)
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("expected %s: %v", tc.failure, err)
				}
				after, err := readScaffoldSnapshot(dir)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("rejected change published", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"*Child", "[]*Child", "*Child"} {
				plan.Views[0].Fields[0].Type = want
				if _, err = EmitScaffold(dir, plan); err != nil {
					t.Fatal(err)
				}
				parsed, err := (xshape.SourceParser{}).ParseFile(path)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, field := range parsed.Fields {
					if field.Owner == "Row" && len(field.Names) == 1 && field.Names[0] == "Child" {
						found = true
						if field.TypeExpr != want {
							t.Fatalf("emitted %s want %s", field.TypeExpr, want)
						}
					}
				}
				if !found {
					t.Fatal("missing holder")
				}
			}
			if tc.edit == "comment" {
				data, err = os.ReadFile(path)
				if err != nil || !strings.Contains(string(data), "application-owned") || !strings.Contains(string(data), "Authored string") {
					t.Fatal("authored content lost", err)
				}
			}
		})
	}
}
