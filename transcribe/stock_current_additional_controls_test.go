package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"os"
	"strings"
	"testing"
)

func TestStockCurrentMissingNestedAuthorityAndNonstandardReadonly(t *testing.T) {
	for name, want := range map[string]string{
		"authority-missing-nested-aux": `auxiliary lookup view::Chain|namespace:Chain with nested relations requires explicit authored current authority`,
		"authority-nonstandard-aux":    `auxiliary lookup view::Carrier|namespace:Carrier with nested relations requires explicit authored current authority`,
	} {
		t.Run(name, func(t *testing.T) {
			source, root := stockExpandedCurrentFixture(t, name)
			before := source.Text
			_, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("intended authority stage %q got %v", want, err)
			}
			if source.Text != before {
				t.Fatal("source mutation")
			}
			stockCurrentNoEmission(t, root)
			t.Logf("unchanged required authority: %v", err)
		})
	}
}

func TestStockCurrentAbsentPhysicalChildUsesImmediateAuxCurrent(t *testing.T) {
	for _, name := range []string{"absent-physical-current", "authority-readonly-child"} {
		t.Run(name, func(t *testing.T) {
			source, root := stockExpandedCurrentFixture(t, name)
			stockAssertCompleteAuthority(t, source, root, map[string]bool{"Probe": false, "Carrier": true, "Children": false}, map[string]string{"Carrier": "Probe", "Children": "Carrier"})
			generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, f := range generated.Result.Files {
				if strings.HasSuffix(f.Path, "current_children.sql") {
					b, e := os.ReadFile(f.Path)
					if e != nil {
						t.Fatal(e)
					}
					if !strings.Contains(string(b), `$Unsafe.ProjectCurrentChildrenParentKeys($CurrentCarrier)`) || strings.Contains(string(b), `ParentKeys($CurrentProbe)`) {
						t.Fatalf("child scope not immediate auxiliary: %s", b)
					}
					found = true
					t.Logf("derived child authorized by immediate CurrentCarrier: %s", b)
				}
			}
			if !found {
				t.Fatal("missing derived CurrentChildren SQL")
			}
		})
	}
}

func TestStockCurrentWildcardEvolutionThroughAuxiliaryParent(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	schema, err := stockCurrentResources.ReadFile("stock_current_resources/expanded-schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(schema), ";") {
		if strings.TrimSpace(stmt) != "" {
			if err = db.ExecStatements(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/stockcurrentfixture"}).Write(t, root)
	text, err := stockCurrentResources.ReadFile("stock_current_resources/nested.dql")
	if err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "Probe", Scope: "github.com/viant/datly/stockcurrentfixture/source", Connector: "main", Text: string(text), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	first, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if stockArtifactsContain(t, first, "EvolvedNote") {
		t.Fatal("unexpected future column")
	}
	if err = db.ExecStatements(ctx, "ALTER TABLE children ADD COLUMN evolved_note TEXT"); err != nil {
		t.Fatal(err)
	}
	next, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	if !stockArtifactsContain(t, next, "EvolvedNote") || !stockArtifactsContain(t, next, "evolved_note") {
		t.Fatal("wildcard schema evolution absent")
	}
	if source.Text != string(text) {
		t.Fatal("DQL changed")
	}
	stockAssertCompleteAuthority(t, source, root, map[string]bool{"Probe": false, "Carrier": true, "Children": false}, map[string]string{"Carrier": "Probe", "Children": "Carrier"})
	t.Log("wildcard schema changed body/Current generated contracts without DQL/Go edits")
}
func stockArtifactsContain(t *testing.T, g *GeneratedPackage, word string) bool {
	t.Helper()
	for _, f := range g.Result.Files {
		if strings.HasSuffix(f.Path, ".go") {
			b, e := os.ReadFile(f.Path)
			if e != nil {
				t.Fatal(e)
			}
			if strings.Contains(string(b), word) {
				return true
			}
		}
	}
	return false
}
