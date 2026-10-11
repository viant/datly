package identifier

import (
	"github.com/viant/datly/constant"
	"testing"
)

func TestImmutableIdentifierTargetsAndProtectedSource(t *testing.T) {
	const source = "SELECT '$project.ds.records' FROM `$project.ds.records` -- $project.ds.records"
	for _, project := range []string{"first", "second"} {
		values, err := constant.New(map[string]string{"project": project, "Table": "main.records"})
		if err != nil {
			t.Fatal(err)
		}
		renderer := Renderer{Values: values}
		got, err := renderer.Render(source)
		want := "SELECT '$project.ds.records' FROM `" + project + ".ds.records` -- $project.ds.records"
		if err != nil || got != want {
			t.Fatalf("protected source: %q %v", got, err)
		}
		target, err := renderer.Identifier("$Unsafe.Table")
		if err != nil || target != "main.records" {
			t.Fatalf("full target: %q %v", target, err)
		}
		target, err = renderer.Identifier("`$project.ds.records`")
		if err != nil || target != "`"+project+".ds.records`" {
			t.Fatalf("fragment target: %q %v", target, err)
		}
		local := "#set($project = $Input.Project) SELECT $project.ds.records"
		got, err = renderer.Render(local)
		if err != nil || got != local {
			t.Fatalf("local variable acquired constant authority: %q %v", got, err)
		}
	}
	for _, value := range []string{"", "records; DELETE FROM records", "records WHERE 1=1"} {
		values, err := constant.New(map[string]string{"Table": value})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = (Renderer{Values: values}).Identifier("$Unsafe.Table"); err == nil {
			t.Fatalf("unsafe target admitted: %q", value)
		}
	}
}
