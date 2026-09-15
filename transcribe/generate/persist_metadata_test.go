package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestProjectionMetadataOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, edit, targetType, targetTag string
		explicit, fail                    bool
	}{
		{name: "add invariant", targetTag: `sqlx:"start" invariant:"Window"`},
		{name: "nullable", targetType: "*int"},
		{name: "cast", targetType: "string", explicit: true},
		{name: "edited tag", edit: "tag", targetTag: `sqlx:"start" invariant:"Window"`, fail: true},
		{name: "edited tag equals proposal", edit: "proposal", targetTag: `sqlx:"start" invariant:"Window"`, fail: true},
		{name: "edited type before metadata", edit: "type", targetTag: `sqlx:"start" invariant:"Window"`, fail: true},
		{name: "edited type with unchanged cast", edit: "type", targetType: "int", explicit: true, fail: true},
		{name: "edited type before cast", edit: "type", targetType: "string", explicit: true, fail: true},
		{name: "edited type equals cast", edit: "type", targetType: "*int", explicit: true, fail: true},
		{name: "edited tag before inferred type", edit: "tag", targetType: "*int", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Start", Type: "int", Tag: `sqlx:"start"`}}}}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "records.go")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data) + "\n// authored method\nfunc (r Row) Note() string { return \"kept\" }\n"
			switch tc.edit {
			case "tag":
				text = strings.Replace(text, `sqlx:"start"`, `sqlx:"start" custom:"kept"`, 1)
			case "proposal":
				text = strings.Replace(text, `sqlx:"start"`, tc.targetTag, 1)
			case "type":
				text = strings.Replace(text, "Start int", "Start *int", 1)
			}
			if err = os.WriteFile(path, []byte(text), 0644); err != nil {
				t.Fatal(err)
			}
			field := &plan.Views[0].Fields[0]
			if tc.targetType != "" {
				field.Type, field.ExplicitType = tc.targetType, tc.explicit
			}
			if tc.targetTag != "" {
				field.Tag = tc.targetTag
			}
			plan.Description = "updated router too"
			before, err := readScaffoldSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = plan.ValidateDestination(dir); (err != nil) != tc.fail {
				t.Fatalf("preflight: %v", err)
			}
			_, err = EmitScaffold(dir, plan)
			if tc.fail {
				if err == nil || !strings.Contains(err.Error(), "customized type or tag") {
					t.Fatalf("conflict: %v", err)
				}
				after, err := readScaffoldSnapshot(dir)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("failed metadata edit partially published", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err = os.ReadFile(path)
			if err != nil || !strings.Contains(string(data), "// authored method") || !strings.Contains(string(data), field.Tag) || !strings.Contains(string(data), "Start "+field.Type) {
				t.Fatalf("updated source: %s\n%v", data, err)
			}
			before, err = readScaffoldSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			after, err := readScaffoldSnapshot(dir)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("identical generation changed package", err)
			}
		})
	}
}
