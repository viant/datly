package presencegen

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureSource = `package fixture

type Root struct {
	Id int ` + "`sqlx:\"ID,primaryKey\"`" + `
	Name *string ` + "`sqlx:\"NAME\"`" + `
	Count *int ` + "`sqlx:\"COUNT\"`" + `
	Codes []int ` + "`sqlx:\"CODES\"`" + `
	Children []*Child ` + "`sqlx:\"-\"`" + `
	Has *RootHas ` + "`setMarker:\"true\" sqlx:\"-\"`" + `
}
type RootHas struct { Id, Name, Count, Codes, Children bool }
type Child struct {
	Id int ` + "`sqlx:\"ID,primaryKey\"`" + `
	Value *int ` + "`sqlx:\"VALUE\"`" + `
	Has *ChildHas ` + "`setMarker:\"true\" sqlx:\"-\"`" + `
}
type ChildHas struct { Id, Value bool }
`

func TestGenerate(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "entity.go"), []byte(fixtureSource), 0o644); err != nil {
		t.Fatal(err)
	}
	generated, err := generate(dir, "Root", filepath.Join(dir, "root_presence.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	checks := []string{
		"func (r *Root) SynchronizePresentMarker(prev *Root)",
		"func (c *Child) SynchronizePresentMarker(prev *Child)",
		"has.Id = present.Id",
		"present.Name && !(r.Name == prev.Name",
		"presenceSlices.Equal(r.Codes, prev.Codes)",
		"present.Children && rootChildrenPresenceChanged",
		"map[int]*Child",
		"previousCount[identity] != 1",
	}
	for _, check := range checks {
		if !strings.Contains(text, check) {
			t.Fatalf("generated source does not contain %q:\n%s", check, text)
		}
	}
	if strings.Contains(text, "reflect") {
		t.Fatalf("generated source uses reflection:\n%s", text)
	}
	if _, err = parser.ParseFile(token.NewFileSet(), "root_presence.go", generated, parser.AllErrors); err != nil {
		t.Fatalf("generated source is invalid: %v", err)
	}
}

func TestRunWritesDeterministicOutput(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "entity.go"), []byte(fixtureSource), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := Config{SourceDir: dir, Entry: "Root"}
	if err := Run(cfg); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(dir, "root_presence.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err = Run(cfg); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(dir, "root_presence.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("repeated generation changed the output")
	}
}

func TestGenerateRequiresExistingMarker(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "entity.go"), []byte("package fixture\ntype Root struct { Name string }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := generate(dir, "Root", "")
	if err == nil || !strings.Contains(err.Error(), "no existing setMarker Has field") {
		t.Fatalf("expected marker error, got %v", err)
	}
}

func TestRunRecursiveDeduplicatesSharedEntities(t *testing.T) {
	root := t.TempDir()
	patchDir := filepath.Join(root, "component", "patch")
	if err := os.MkdirAll(patchDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := `package patch
type Input struct { Data []*Envelope ` + "`parameter:\",kind=body,in=data\"`" + ` }
type Envelope struct { First *First; Second *Second }
type First struct { Id int ` + "`sqlx:\"ID,primaryKey\"`" + `; Children []*Child; Has *FirstHas ` + "`setMarker:\"true\"`" + ` }
type FirstHas struct { Children bool }
type Second struct { Id int ` + "`sqlx:\"ID,primaryKey\"`" + `; Children []*Child; Has *SecondHas ` + "`setMarker:\"true\"`" + ` }
type SecondHas struct { Children bool }
type Child struct { Id int ` + "`sqlx:\"ID,primaryKey\"`" + `; Value *int; Has *ChildHas ` + "`setMarker:\"true\"`" + ` }
type ChildHas struct { Value bool }
`
	if err := os.WriteFile(filepath.Join(patchDir, "entity.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := RunRecursive(root, &output); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(patchDir, "first_presence.go"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(patchDir, "second_presence.go"))
	if err != nil {
		t.Fatal(err)
	}
	combined := string(first) + string(second)
	if count := strings.Count(combined, "func (c *Child) SynchronizePresentMarker"); count != 1 {
		t.Fatalf("shared Child method count = %d, want 1\n%s", count, combined)
	}
}

func TestGeneratePreservesCompositePrimaryKeyMarkers(t *testing.T) {
	dir := t.TempDir()
	source := `package fixture
type Entry struct {
	AccountId int ` + "`sqlx:\"ACCOUNT_ID,primaryKey\"`" + `
	Code string ` + "`sqlx:\"CODE,primaryKey\"`" + `
	Value *int ` + "`sqlx:\"VALUE\"`" + `
	Has *EntryHas ` + "`setMarker:\"true\" sqlx:\"-\"`" + `
}
type EntryHas struct { AccountId, Code, Value bool }
`
	if err := os.WriteFile(filepath.Join(dir, "entity.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	generated, err := generate(dir, "Entry", filepath.Join(dir, "entry_presence.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for _, expected := range []string{"has.AccountId = present.AccountId", "has.Code = present.Code"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("generated source does not preserve %q:\n%s", expected, text)
		}
	}
	if strings.Contains(text, "e.AccountId != prev.AccountId") || strings.Contains(text, "e.Code != prev.Code") {
		t.Fatalf("generated source compares a primary key:\n%s", text)
	}
}
