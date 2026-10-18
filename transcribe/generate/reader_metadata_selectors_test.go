package generate

import (
	"encoding/json"
	"github.com/viant/bindly/resource"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

func TestReaderMetadataSideTagsStableAcrossCompanionReload(t *testing.T) {
	input := reviewLinkedEligibilityInput(t, false)
	// This selector case inspects current package files. Use the owning module
	// rather than the nested-module fixture used by metadata-only cases.
	oldDirectory := filepath.Join(input.ProjectRoot, "generate")
	directory := filepath.Join(input.ProjectRoot, "transcribe", "generate")
	require.NoError(t, os.MkdirAll(filepath.Dir(directory), 0700))
	require.NoError(t, os.Rename(oldDirectory, directory))
	require.NoError(t, os.WriteFile(filepath.Join(input.ProjectRoot, "go.mod"), []byte("module github.com/viant/datly\n\ngo 1.25.0\n"), 0600))
	input.Resources = resource.New()
	require.NoError(t, input.Resources.Register(readableResourceNamespace(input.TargetPackage, "Records"), os.DirFS(directory)))
	optional := false
	for _, entry := range []struct {
		name, source, typ string
		property          spec.SelectorProperty
	}{
		{"Fields", "r_fields", "[]string", spec.SelectorPropertyFields},
		{"Page", "page", "int", spec.SelectorPropertyPage},
		{"Limit", "r_limit", "int", spec.SelectorPropertyLimit},
		{"Offset", "r_offset", "int", spec.SelectorPropertyOffset},
		{"OrderBy", "r_orderby", "[]string", spec.SelectorPropertyOrderBy},
	} {
		input.Component.Parameters = append(input.Component.Parameters, &spec.Parameter{Name: entry.name, Source: spec.BindSource{Kind: "query", Name: entry.source}, TypeExpr: entry.typ, Required: &optional, Cacheable: &optional, Tag: `json:",omitempty" description:"preserve selector tag"`, QuerySelector: &spec.QuerySelectorBinding{View: "records", Property: entry.property}})
	}
	original, err := json.Marshal(input.Component)
	require.NoError(t, err)
	first, err := New(input).Plan()
	require.NoError(t, err)
	metadata := func(plan *Plan) string {
		for _, file := range plan.Resources.Files {
			if strings.HasPrefix(file.Path, "metadata/") {
				return file.Content
			}
		}
		t.Fatal("reader metadata not emitted")
		return ""
	}
	firstMetadata := metadata(first)
	reloaded := input
	reloaded.Component = input.Component.Clone()
	canonical := map[string]string{}
	for _, field := range first.QuerySelectors {
		parsed, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: reflect.StructTag(field.Tag)})
		require.NoError(t, err)
		identity := (&spec.Parameter{Name: parsed.Binding.Name, Source: spec.BindSource{Kind: parsed.Binding.Location.Kind, Name: parsed.Binding.Location.In}}).Identity()
		canonical[identity] = field.Tag
	}
	require.Len(t, canonical, 5)
	for _, parameter := range reloaded.Component.Parameters {
		if value, found := canonical[parameter.Identity()]; found {
			parameter.Tag = value
		}
	}
	second, err := New(reloaded).Plan()
	require.NoError(t, err)
	require.Equal(t, firstMetadata, metadata(second), "compiled companion tag enrichment must not change the native metadata")
	var document struct {
		Component *spec.Component `json:"component"`
	}
	require.NoError(t, json.Unmarshal([]byte(firstMetadata), &document))
	require.NotNil(t, document.Component)
	for _, parameter := range document.Component.Parameters {
		if expected, found := canonical[parameter.Identity()]; found {
			require.Equal(t, expected, parameter.Tag)
			require.Contains(t, parameter.Tag, `description:"preserve selector tag"`)
		}
	}
	after, err := json.Marshal(input.Component)
	require.NoError(t, err)
	require.Equal(t, original, after, "metadata rendering must not mutate authored input")
}
