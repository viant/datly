package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/testdata/outputoverlay"
	"reflect"
)

// Current DQL must regenerate a renamed authored linked row.
func TestDiscoveryRefreshAfterAuthoredLinkedFieldRename(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/transcribe/testdata"}).Write(t, root)
	for _, path := range []string{"fixture.go", "holder.go", "sql/records.sql", "sql/metric.sql"} {
		b, e := os.ReadFile(filepath.Join("testdata/outputoverlay", path))
		if e != nil {
			t.Fatal(e)
		}
		writeSourceFile(t, root, "outputoverlay/"+path, string(b))
	}
	text := strings.Replace(outputOverlayDQL, "#package('example.com/generated/reader')", "#package('outputoverlay')", 1)
	text = strings.Replace(text, "SELECT r.*,type(r,'legacy.Row'),grouping_enabled(r),set_limit(r,3),use_connector(r,'current')\nFROM (SELECT ID,NAME FROM records) r", "SELECT r.*,metric.*,type(r,'legacy.Row'),type(metric,'legacy.Metric'),cardinality(metric,'one'),grouping_enabled(r),set_limit(r,3),use_connector(r,'current'),use_cache(r,'root_cache'),set_limit(metric,2),use_connector(metric,'nested'),use_cache(metric,'nested_cache')\nFROM (SELECT ID,NAME FROM records) r\nLEFT JOIN (SELECT ID,AMOUNT FROM metrics) metric ON metric.ID=r.ID", 1)
	result, e := compileOutputOverlay(t, text, nil)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = (Generator{Operation: "get", Language: HandlerGo}).Generate(ctx, GenerationRequest{Compiled: result, Destination: root}); e != nil {
		t.Fatal("initial generation:", e)
	}
	if e = os.Remove(filepath.Join(root, "outputoverlay/holder.go")); e != nil {
		t.Fatal(e)
	}
	writeSourceFile(t, root, "outputoverlay/selectors.go", `package outputoverlay
 func(RecordsComponent)DatlyQuerySelectors()selectors{panic("must not execute")}`)
	text = strings.Replace(text, "#package('outputoverlay')", "#package('github.com/viant/datly/transcribe/testdata/outputoverlay')", 1)
	text = strings.Replace(text, "#setting($_ = $output_type('legacy.Output'))", "#setting($_ = $input_type('legacy.Input'))\n#setting($_ = $output_type('legacy.Output'))", 1)
	writeSourceFile(t, root, "source/query.dql", text)
	d := &Discovery{BaseDir: root, Include: []string{"github.com/viant/datly/transcribe/testdata/source"}, SourceFile: "query.dql"}
	if _, e = d.Compile(ctx); e != nil {
		t.Fatal("pre-rename discovery:", e)
	}
	f := filepath.Join(root, "outputoverlay/fixture.go")
	b, e := os.ReadFile(f)
	if e != nil {
		t.Fatal(e)
	}
	before := `Name   *string ` + "`sqlx:\"NAME\"`"
	after := `DisplayName *string ` + "`sqlx:\"NAME\" json:\"name\"`"
	if !strings.Contains(string(b), before) {
		t.Fatal("authored field missing")
	}
	if e = os.WriteFile(f, []byte(strings.Replace(string(b), before, after, 1)), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = d.Compile(ctx); e != nil {
		t.Fatal("AST-only current DQL compile:", e)
	}
	writeSourceFile(t, root, "outputoverlay/rename_repro_test.go", `package outputoverlay

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/transcribe"
	"github.com/viant/x"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCompiledLinkedRename(t *testing.T) {
	root := os.Getenv("READER_REFRESH_ROOT")
	ctx := context.Background()
	found, e := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[RecordsComponent]().PkgPath()}, nil)
	if e != nil {
		t.Fatal(e)
	}
	var source *bootstrap.RouteSource
	for _, s := range found.Components {
		if s.Tag.Name == "Records" {
			source = s
			break
		}
	}
	if source == nil {
		t.Fatal("source missing")
	}
	locator := source.Tag.ReaderMetadata
	if os.Getenv("READER_REFRESH_RELOAD") == "1" {
		source.Dir = ""
		if _, e = source.Resolve(reflect.TypeFor[Input](), reflect.TypeFor[Output]()); e != nil {
			t.Fatal("fresh compiled metadata reload:", e)
		}
		field, _ := reflect.TypeFor[Row]().FieldByName("DisplayName")
		if field.Tag.Get("sqlx") != "NAME" || field.Tag.Get("json") != "name" {
			t.Fatal("renamed contract tags changed")
		}
		return
	}
	source.Dir = filepath.Join(root, "outputoverlay")
	if _, e = source.Resolve(reflect.TypeFor[Input](), reflect.TypeFor[Output]()); e == nil || !strings.Contains(e.Error(), "contract closure differs") {
		t.Fatalf("old runtime metadata must reject renamed contract: %v", e)
	}
	r := x.NewRegistry()
	for _, typ := range []reflect.Type{reflect.TypeFor[Input](), reflect.TypeFor[Output](), reflect.TypeFor[Row](), reflect.TypeFor[Metric]()} {
		r.Register(x.NewType(typ))
	}
	d := &transcribe.Discovery{BaseDir: root, Include: []string{"github.com/viant/datly/transcribe/testdata/source"}, SourceFile: "query.dql", Registry: r}
	packageOnly := *d
	packageOnly.Include = []string{"github.com/viant/datly/transcribe/testdata/outputoverlay"}
	packageOnly.SourceFile = ""
	if _, e = packageOnly.Compile(ctx); e == nil || !strings.Contains(e.Error(), "contract closure differs") {
		t.Fatalf("old package-only metadata must reject renamed contract: %v", e)
	}
	// A co-selected Go holder must be claimed by its current authored DQL.
	d.Include = append(d.Include, "github.com/viant/datly/transcribe/testdata/outputoverlay")
	before, e := os.ReadFile(filepath.Join(root, "outputoverlay/metadata/records_reader.json"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = d.Generate(ctx, root); e != nil {
		t.Fatal("refresh generation:", e)
	}
	if source.Tag.ReaderMetadata != locator {
		t.Fatal("original holder locator mutated")
	}
	after, e := os.ReadFile(filepath.Join(root, "outputoverlay/metadata/records_reader.json"))
	if e != nil {
		t.Fatal(e)
	}
	var oldDoc, newDoc struct{ ABI string }
	if e = json.Unmarshal(before, &oldDoc); e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(after, &newDoc); e != nil {
		t.Fatal(e)
	}
	if oldDoc.ABI == newDoc.ABI {
		t.Fatal("ABI not refreshed")
	}
	if _, e = source.Resolve(reflect.TypeFor[Input](), reflect.TypeFor[Output]()); e != nil {
		t.Fatal("new metadata source reload:", e)
	}
}
`)
	sdk, e := filepath.Abs("..")
	if e != nil {
		t.Fatal(e)
	}
	replacements := map[string]string{}
	e = filepath.WalkDir(filepath.Join(root, "outputoverlay"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(filepath.Join(root, "outputoverlay"), path)
		if e != nil {
			return e
		}
		replacements[filepath.Join(sdk, "transcribe/testdata/outputoverlay", rel)] = path
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	empty := filepath.Join(root, "retired_holder.go")
	if e = os.WriteFile(empty, []byte("package outputoverlay\n"), 0600); e != nil {
		t.Fatal(e)
	}
	replacements[filepath.Join(sdk, "transcribe/testdata/outputoverlay/holder.go")] = empty
	overlay, e := json.Marshal(map[string]any{"Replace": replacements})
	if e != nil {
		t.Fatal(e)
	}
	op := filepath.Join(root, "compiled-overlay.json")
	if e = os.WriteFile(op, overlay, 0600); e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command("go", "test", "-overlay", op, "./transcribe/testdata/outputoverlay", "-run", "^TestCompiledLinkedRename$", "-count=1", "-v")
	cmd.Dir = sdk
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on", "READER_REFRESH_ROOT="+root)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("compiled linked current DQL regeneration: %v\n%s", e, out)
	}
	cmd = exec.Command(cmd.Path, cmd.Args[1:]...)
	cmd.Dir = sdk
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on", "READER_REFRESH_ROOT="+root, "READER_REFRESH_RELOAD=1")
	out, e = cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("fresh compiled metadata reload: %v\n%s", e, out)
	}
}

func TestPackageDeclarationsKeepOriginalReaderLocator(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[outputoverlay.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	source := *found.Components[0]
	source.Tag.ReaderMetadata = "old:metadata/records_reader.json"
	grouped, err := bootstrap.GroupPackageComponentSources([]*bootstrap.RouteSource{&source}, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := source.Tag.ReaderMetadata
	component, err := resolvePackageDeclarations(grouped[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if source.Tag.ReaderMetadata != before || grouped[0].Routes[0].Tag.ReaderMetadata != before {
		t.Fatal("original metadata locator mutated")
	}
	if component.ReaderGraph != nil {
		t.Fatal("declarations claimed prior executable graph")
	}
	if _, err = grouped[0].ResolveDescriptors(nil); err == nil {
		t.Fatal("ordinary descriptor resolution bypassed missing old metadata")
	}
}
