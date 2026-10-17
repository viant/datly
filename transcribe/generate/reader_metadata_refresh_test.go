package generate

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/spec"
)

func reviewAdmittedReader(t *testing.T, changeDocument func(*readergraph.Document)) Input {
	t.Helper()
	input := reviewLinkedEligibilityInput(t, false)
	plan, err := New(input).Plan()
	if err != nil {
		t.Fatal(err)
	}
	var document *readergraph.Document
	assets := map[string][]byte{}
	files := fstest.MapFS{}
	for _, file := range plan.Resources.Files {
		files[file.Path] = &fstest.MapFile{Data: []byte(file.Content)}
		if strings.HasPrefix(file.Path, "metadata/") {
			document, err = readergraph.StrictDecode([]byte(file.Content))
			if err != nil {
				t.Fatal(err)
			}
			input.ReaderMetadataURI = plan.Resources.Namespace + ":" + file.Path
		}
	}
	if document == nil {
		t.Fatal("missing initial metadata")
	}
	if changeDocument != nil {
		changeDocument(document)
	}
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	_, metadataPath, _ := strings.Cut(input.ReaderMetadataURI, ":")
	files[metadataPath] = &fstest.MapFile{Data: body}
	for path, file := range files {
		assets[plan.Resources.Namespace+":"+path] = file.Data
	}
	store := resource.New()
	if err := store.Register(plan.Resources.Namespace, files); err != nil {
		t.Fatal(err)
	}
	input.Resources = store
	in, err := input.TypeResolver.Descriptor(document.Input)
	if err != nil {
		t.Fatal(err)
	}
	out, err := input.TypeResolver.Descriptor(document.Output)
	if err != nil {
		t.Fatal(err)
	}
	input.ReaderMetadataSource, err = readergraph.AdmitWithLookup(document.Component, in, out, assets, input.TypeResolver.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func reviewTree(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	result := map[string][32]byte{}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[relative] = sha256.Sum256(body)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return result
}
func reviewEqualTree(t *testing.T, before, after map[string][32]byte) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("destination tree membership changed")
	}
	for path, sum := range before {
		if after[path] != sum {
			t.Fatalf("destination changed: %s", path)
		}
	}
}

func TestReaderMetadataNoSelectorRefreshRequiresPriorBinding(t *testing.T) {
	input := reviewAdmittedReader(t, nil)
	baseline, err := readergraph.Bytes(input.ReaderMetadataSource)
	if err != nil {
		t.Fatal(err)
	}
	marker := input.ReaderMetadataSource.ReaderGraph
	limit := 7
	input.Component.RootView.Source.Controls = &spec.ViewControls{Limit: &limit}
	if input.Component.ReaderGraph != nil || input.Component.SelectorCompanion != nil {
		t.Fatal("fixture must represent detached no-selector DQL overlay")
	}
	before := reviewTree(t, input.ProjectRoot)
	plan, err := New(input).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.ReaderMetadata == "" {
		t.Fatal("mandatory no-selector refresh was dropped")
	}
	found := false
	for _, file := range plan.Resources.Files {
		if strings.HasPrefix(file.Path, "metadata/") {
			found = true
			if !strings.Contains(file.Content, `"limit": 7`) {
				t.Fatal("refreshed graph control lost")
			}
		}
	}
	if !found {
		t.Fatal("refreshed document missing")
	}
	after, err := readergraph.Bytes(input.ReaderMetadataSource)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baseline, after) || input.ReaderMetadataSource.ReaderGraph != marker {
		t.Fatal("admitted baseline or marker mutated")
	}
	reviewEqualTree(t, before, reviewTree(t, input.ProjectRoot))
}

func TestReaderMetadataRefreshRejectsUnsupportedOrSubstitutedAuthority(t *testing.T) {
	for _, name := range []string{"current marker without carrier", "URI without carrier", "disabled resources", "static", "route", "independent", "handler", "missing URI", "foreign URI", "missing resources", "changed graph", "changed current marker", "foreign input contract", "foreign holder", "foreign field", "authored resource declaration"} {
		t.Run(name, func(t *testing.T) {
			var mutate func(*readergraph.Document)
			if name == "foreign holder" {
				mutate = func(d *readergraph.Document) { d.Holder = "ForeignComponent" }
			}
			if name == "foreign field" {
				mutate = func(d *readergraph.Document) { d.Field = "Foreign" }
			}
			input := reviewAdmittedReader(t, mutate)
			originalBaseline := input.ReaderMetadataSource
			switch name {
			case "current marker without carrier":
				input.Component = input.ReaderMetadataSource.Clone()
				input.ReaderMetadataSource = nil
				input.ReaderMetadataURI = ""
			case "URI without carrier":
				input.ReaderMetadataSource = nil
			case "disabled resources":
				input.SQLResources = false
			case "static":
				input.Component.Static = &spec.StaticContent{Path: "/assets", Namespace: "assets", Root: "."}
			case "route":
				input.Component.Routes[0].Method = "POST"
			case "independent":
				input.Component.Views = []*spec.View{{Name: "Other"}}
			case "handler":
				input.Component.Routes[0].Handler = "Other"
			case "missing URI":
				input.ReaderMetadataURI = ""
			case "foreign URI":
				input.ReaderMetadataURI = "foreign:metadata/records_reader.json"
			case "missing resources":
				input.Resources = resource.New()
			case "changed graph":
				input.ReaderMetadataSource.RootView.Columns = nil
			case "changed current marker":
				input.Component = input.ReaderMetadataSource.Clone()
				input.Component.RootView.Columns = nil
			case "foreign input contract":
				input.Contracts.Input.DescriptorKey = "foreign.Input"
			case "authored resource declaration":
				path := filepath.Join(input.ProjectRoot, "generate", "records_resources.go")
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				body = []byte(strings.TrimPrefix(string(body), generatedHeader("Records")))
				if err = os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			baseline, err := readergraph.Bytes(originalBaseline)
			if err != nil {
				t.Fatal(err)
			}
			marker := originalBaseline.ReaderGraph
			before := reviewTree(t, input.ProjectRoot)
			if _, err := New(input).Plan(); err == nil {
				t.Fatal("unsupported or substituted admitted refresh accepted")
			}
			after, err := readergraph.Bytes(originalBaseline)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(baseline, after) || originalBaseline.ReaderGraph != marker {
				t.Fatal("failed refresh mutated caller baseline")
			}
			reviewEqualTree(t, before, reviewTree(t, input.ProjectRoot))
		})
	}
}

func TestReaderMetadataIneligibleUnmarkedLeavesStaleAssetUntouched(t *testing.T) {
	input := reviewLinkedEligibilityInput(t, true)
	directory := filepath.Join(input.ProjectRoot, "generate")
	pure := reviewLinkedEligibilityInput(t, false)
	purePlan, err := New(pure).Plan()
	if err != nil {
		t.Fatal(err)
	}
	var stale []byte
	for _, file := range purePlan.Resources.Files {
		if strings.HasPrefix(file.Path, "metadata/") {
			stale = []byte(file.Content)
		}
	}
	if len(stale) == 0 {
		t.Fatal("missing valid stale metadata fixture")
	}
	path := filepath.Join(directory, "metadata", "records_reader.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, stale, 0600); err != nil {
		t.Fatal(err)
	}
	resourcePath := filepath.Join(directory, "records_resources.go")
	body, err := os.ReadFile(resourcePath)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), `//go:embed "sql/records.sql" "sql/authorization.sql"`, `//go:embed "sql/records.sql" "sql/authorization.sql" "metadata/records_reader.json"`, 1))
	if err = os.WriteFile(resourcePath, body, 0600); err != nil {
		t.Fatal(err)
	}
	before := reviewTree(t, input.ProjectRoot)
	plan, err := New(input).Plan()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range plan.Resources.Files {
		if file.Path == "metadata/records_reader.json" {
			found = true
			if file.Content != string(stale) {
				t.Fatal("stale unmarked asset rewritten")
			}
		}
	}
	if !found {
		t.Fatal("stale unmarked asset removed")
	}
	if plan.ReaderMetadata != "" || len(plan.readerMetadataPrevious) != 0 {
		t.Fatal("stale bytes granted metadata authority")
	}
	reviewEqualTree(t, before, reviewTree(t, input.ProjectRoot))
}

func TestReaderMetadataRuntimeShapeDiscoveryRetainsBaseline(t *testing.T) {
	input := reviewAdmittedReader(t, nil)
	baseline, err := readergraph.Bytes(input.ReaderMetadataSource)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := New(input).plan(false)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Resources != nil || plan.ReaderMetadata != "" {
		t.Fatal("runtime type discovery unexpectedly prepared publication")
	}
	input.SQLResources = false
	if _, err := New(input).plan(false); err != nil {
		t.Fatalf("runtime discovery mistaken for SQL publication: %v", err)
	}
	input.Component.Static = &spec.StaticContent{Path: "/assets", Namespace: "assets", Root: "."}
	if _, err := New(input).plan(false); err == nil {
		t.Fatal("static transition bypassed prior authority in runtime discovery")
	}

	after, err := readergraph.Bytes(input.ReaderMetadataSource)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baseline, after) {
		t.Fatal("runtime type discovery mutated baseline")
	}
}
