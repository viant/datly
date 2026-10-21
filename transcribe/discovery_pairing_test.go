package transcribe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

const declaredPackageFixture = `package records
import xdatly "github.com/viant/xdatly"
type Input struct { ID int ` + "`parameter:\",kind=query,in=id\"`" + ` }
type OtherInput struct { Name string }
type Row struct { ID int ` + "`sqlx:\"id\"`" + ` }
type Output struct { Data []*Row ` + "`parameter:\",kind=output,in=view\" view:\"records\" sql:\"SELECT id FROM records\"`" + ` }
type Holder struct { Contract xdatly.Component[Input,Output] ` + "`component:\"Records,path=/records,method=GET,view=records\"`" + ` }
type Other struct { Contract xdatly.Component[OtherInput,Output] ` + "`component:\"Other,path=/other,method=GET,view=records\"`" + ` }
`

const declaredPackageDQL = `#package('example.com/generated/api/records')
#import('records','example.com/generated/api/records')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('records.Input'))
#setting($_ = $output_type('records.Output'))
SELECT records.*,type(records,'Row') FROM (${embed:sql/records.sql}) records
`

func declaredPackageProject(t *testing.T, component, text string) (string, *Discovery) {
	t.Helper()
	base := t.TempDir()
	base, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "api/records/component.go", component)
	writeSourceFile(t, base, "source/query.dql", text)
	writeSourceFile(t, base, "source/sql/records.sql", "SELECT id FROM records WHERE id > 17")
	return base, &Discovery{BaseDir: base, Include: []string{"example.com/generated/source"}, SourceFile: "query.dql"}
}

func TestDiscoveryDeclaredPackagePairsDistinctSource(t *testing.T) {
	base, discovery := declaredPackageProject(t, declaredPackageFixture, declaredPackageDQL)
	project, err := discovery.Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 {
		t.Fatalf("pairing exposed unrelated components: %d", len(project.Components))
	}
	result := project.Components[0]
	shared := result.Source.preparation
	compilation, err := prepareCompilation(context.Background(), result.Source)
	if err != nil || shared == nil || compilation.prepared != shared.prepared || compilation.result.Source.preparation != shared {
		t.Fatalf("discovery and compiler did not share preparation: %v", err)
	}
	if result.Source.Scope != "example.com/generated/api/records" || result.Source.Name != "Records" || result.Source.Path != filepath.Join(base, "source/query.dql") || result.Source.BaseDir() != filepath.Join(base, "source") {
		t.Fatalf("logical identity or physical source changed: %+v", result.Source)
	}
	if result.Contracts.Input.DescriptorKey != "example.com/generated/api/records.Input" || result.Contracts.Output.DescriptorKey != "example.com/generated/api/records.Output" {
		t.Fatalf("contracts were not paired: %+v", result.Contracts)
	}
	adjacent, err := result.Source.Resources.ReadFile("sql/records.sql")
	if err != nil || string(adjacent) != "SELECT id FROM records WHERE id > 17" {
		t.Fatalf("original adjacent SQL default changed: %s %v", adjacent, err)
	}
	// An explicitly included holder must still compile exactly once after pairing.
	discovery.Include = append(discovery.Include, "example.com/generated/api/records")
	project, err = discovery.Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 {
		t.Fatalf("basename selection or paired bookkeeping changed: %d", len(project.Components))
	}
}

func TestDiscoveryDeclaredPackageParameterizedPreparation(t *testing.T) {
	text := strings.Replace(declaredPackageDQL, "${embed:sql/records.sql}", `${embed({"bound":"17"}):sql/records.sql}`, 1)
	base, discovery := declaredPackageProject(t, declaredPackageFixture, text)
	writeSourceFile(t, base, "source/sql/records.sql", "SELECT id FROM records WHERE id > ${bound}")
	project, err := discovery.Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := project.Components[0]
	shared := result.Source.preparation
	if shared == nil || shared.originalText != text || shared.sourceMap.original != text || strings.Contains(result.Source.Text, "${embed(") || result.Source.Path != filepath.Join(base, "source/query.dql") {
		t.Fatal("parameterized preparation lost original source or staged SQL")
	}
	if shared.originalResources == shared.resources {
		t.Fatal("parameterized resources were not staged")
	}
	compilation, err := prepareCompilation(context.Background(), result.Source)
	if err != nil || compilation.prepared != shared.prepared || compilation.result.Source.Resources != shared.resources {
		t.Fatalf("parameterized source was expanded or parsed again: %v", err)
	}
	// Diagnostics after an expansion still refer to the authored line and file.
	invalid := text + "\n#setting($_ = $unsupported(true))\n"
	writeSourceFile(t, base, "source/query.dql", invalid)
	_, err = discovery.Compile(context.Background())
	var diagnostic *CompileError
	if !errors.As(err, &diagnostic) || len(diagnostic.Diagnostics) == 0 {
		t.Fatalf("structured compiler diagnostic lost: %v", err)
	}
	span := diagnostic.Diagnostics[0]
	wantLine := strings.Count(invalid[:strings.Index(invalid, "#setting($_ = $unsupported")], "\n") + 1
	if span.Path != filepath.Join(base, "source/query.dql") || span.Span.Start.Line != wantLine || span.Span.Start.Offset != strings.Index(invalid, "#setting($_ = $unsupported") {
		t.Fatalf("diagnostic does not point to original directive: %+v", span)
	}
}

func TestDiscoveryDeclaredPackageRejectsConflictingAuthority(t *testing.T) {
	for _, test := range []struct{ name, component, text, want string }{
		{"contracts", declaredPackageFixture, strings.Replace(declaredPackageDQL, "records.Input", "records.OtherInput", 1), "conflicting"},
		{"route", declaredPackageFixture, strings.Replace(declaredPackageDQL, "'/records'", "'/missing'", 1), "conflicting"},
		{"method", declaredPackageFixture, strings.Replace(declaredPackageDQL, "'GET'", "'POST'", 1), "conflicting"},
		{"multi route", declaredPackageFixture, strings.Replace(declaredPackageDQL, "#setting($_ = $route('/records','GET'))", "#setting($_ = $route('/records','GET'))\n#setting($_ = $route('/missing','GET'))", 1), "conflicting"},
		{"ambiguity", declaredPackageFixture + "\ntype Duplicate struct { Contract xdatly.Component[Input,Output] `component:\"Duplicate,path=/records,method=GET,view=records\"` }\n", declaredPackageDQL, "ambiguous"},
		{"malformed", declaredPackageFixture, strings.Replace(declaredPackageDQL, "#package('example.com/generated/api/records')", "#package(", 1), "invalid #package"},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, discovery := declaredPackageProject(t, test.component, test.text)
			_, err := discovery.Generate(context.Background(), base)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %s, got %v", test.want, err)
			}
			entries, err := os.ReadDir(filepath.Join(base, "api/records"))
			if err != nil || len(entries) != 1 || entries[0].Name() != "component.go" {
				t.Fatalf("failed pairing wrote artifacts: %v %v", entries, err)
			}
		})
	}
}

func TestDiscoveryDeclaredPackageUnrelatedHolderRemainsStandalone(t *testing.T) {
	text := strings.ReplaceAll(declaredPackageDQL, "records.Input", "NewInput")
	text = strings.ReplaceAll(text, "records.Output", "NewOutput")
	text = strings.ReplaceAll(text, "'/records'", "'/new'")
	_, discovery := declaredPackageProject(t, declaredPackageFixture, text)
	project, err := discovery.Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 || project.Components[0].Source.Name != "query" {
		t.Fatalf("unrelated holder claimed new source: %+v", project.Components)
	}
}

func TestDiscoveryDeclaredPackageWithoutHolderRemainsStandalone(t *testing.T) {
	_, discovery := declaredPackageProject(t, "package records\n", declaredPackageDQL)
	if err := os.RemoveAll(filepath.Join(discovery.BaseDir, "api")); err != nil {
		t.Fatal(err)
	}
	// A new generated contract is not required to have an existing holder.
	text := strings.ReplaceAll(declaredPackageDQL, "#import('records','example.com/generated/api/records')\n", "")
	text = strings.ReplaceAll(text, "records.Input", "Input")
	text = strings.ReplaceAll(text, "records.Output", "Output")
	writeSourceFile(t, discovery.BaseDir, "source/query.dql", text)
	project, err := discovery.Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 || project.Components[0].Source.Name != "query" || project.Components[0].Source.Scope != "example.com/generated/source" {
		t.Fatalf("standalone source changed: %+v", project.Components)
	}
}

func TestDiscoverySourceNamePreservesPhysicalResourcesAndPairing(t *testing.T) {
	base, discovery := declaredPackageProject(t, declaredPackageFixture, declaredPackageDQL)
	discovery.SourceName = "Records"
	discovery.Include = append(discovery.Include, "example.com/generated/api/records")
	for i := 0; i < 2; i++ {
		project, err := discovery.Compile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(project.Components) != 1 {
			t.Fatalf("selected source duplicated or renamed sibling: %d", len(project.Components))
		}
		result := project.Components[0]
		if result.Component.Name != "Records" || result.Source.Name != "Records" || result.Source.Path != filepath.Join(base, "source/query.dql") {
			t.Fatalf("source identity changed: %+v", result.Source)
		}
		if result.Component.Routes[0].Path != "/records" {
			t.Fatalf("route changed: %+v", result.Component.Routes)
		}
		adjacent, err := result.Source.Resources.ReadFile("sql/records.sql")
		if err != nil || string(adjacent) != "SELECT id FROM records WHERE id > 17" {
			t.Fatalf("source resource changed: %s %v", adjacent, err)
		}
	}
}
