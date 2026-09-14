package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequiredConstraintRetrofitPreservesOwnership(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(map[bool]string{false: "generated", true: "authored required override"}[custom], func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Enabled", Type: "*bool", Tag: `sqlx:"enabled"`, ExplicitType: true}}}}}
			if _, err := EmitScaffold(dir, plan); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "records.go")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if custom {
				before = []byte(strings.Replace(string(before), `sqlx:"enabled"`, `sqlx:"enabled,required=false"`, 1))
				if err = os.WriteFile(path, before, 0644); err != nil {
					t.Fatal(err)
				}
			}
			plan.Views[0].Fields[0].Tag = `sqlx:"enabled,required=true"`
			if custom {
				if err = plan.ValidateDestination(dir); err == nil {
					t.Fatal("customized tag accepted")
				}
				if _, err = EmitScaffold(dir, plan); err == nil {
					t.Fatal("customized tag overwritten")
				}
				after, err := os.ReadFile(path)
				if err != nil || string(after) != string(before) {
					t.Fatal("failed migration changed source", err)
				}
				return
			}
			for i := 0; i < 2; i++ {
				if err = plan.ValidateDestination(dir); err != nil {
					t.Fatal(err)
				}
				if _, err = EmitScaffold(dir, plan); err != nil {
					t.Fatal(err)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || !strings.Contains(string(after), `required=true`) {
				t.Fatal("constraint not persisted", err)
			}
			manifest, err := readScaffoldManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.ProjectionFields["records.go"].Fields[0].Tag != plan.Views[0].Fields[0].Tag {
				t.Fatal("manifest does not describe generated constraint")
			}
			plan.Views[0].Fields[0].Tag = `sqlx:"enabled"`
			if err = plan.ValidateDestination(dir); err == nil {
				t.Fatal("constraint removal silently accepted")
			}
		})
	}
}
