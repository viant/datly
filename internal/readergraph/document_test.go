package readergraph

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

type testInput struct{ ID int }
type testOutput struct{ Data []*testRow }
type testRow struct{ ID int }

func documentFixture() Document {
	return Document{ABI: strings.Repeat("a", 64), Version: 1, Package: "example.com/reader", Holder: "RecordsComponent", Field: "Contract", Input: "example.com/reader.Input", Output: "example.com/reader.Output", Rows: map[string]string{"view:example.com/reader:records": "example.com/reader.Row"}, Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reader", Name: "Records"}, Name: "Records", Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"r", "ID"}}}}}, RootView: &spec.View{Name: "records", Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int"}}}}}}
}
func TestReaderMetadataStrictJSON(t *testing.T) {
	fixture := documentFixture()
	body, err := json.Marshal(fixture)
	require.NoError(t, err)
	_, err = StrictDecode(body)
	require.NoError(t, err)
	for _, tc := range []struct{ name, old, new string }{
		{"unknown root", "\"version\":1", "\"version\":1,\"unknown\":true"},
		{"duplicate root", "\"version\":1", "\"version\":1,\"version\":1"},
		{"case alias", "\"version\":1", "\"Version\":1"},
		{"case duplicate", "\"version\":1", "\"version\":1,\"Version\":1"},
		{"custom predicate unknown", "\"name\":\"equal\"", "\"name\":\"equal\",\"unknown\":true"},
		{"custom predicate duplicate", "\"name\":\"equal\"", "\"name\":\"equal\",\"name\":\"equal\""},
		{"custom predicate case alias", "\"name\":\"equal\"", "\"Name\":\"equal\""},
		{"unsupported version", "\"version\":1", "\"version\":2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := StrictDecode([]byte(strings.Replace(string(body), tc.old, tc.new, 1)))
			require.Error(t, err)
		})
	}
	_, err = StrictDecode(append(body, []byte(" {}")...))
	require.Error(t, err)
}
func TestReaderMetadataAssociationRejectsSubstitution(t *testing.T) {
	graph := documentFixture().Component
	input := x.NewType(reflect.TypeFor[testInput]())
	output := x.NewType(reflect.TypeFor[testOutput]())
	assets := map[string][]byte{"reader:sql/root.sql": []byte("SELECT ID FROM records")}
	checked, err := Admit(graph, input, output, assets)
	require.NoError(t, err)
	resources := fstest.MapFS{"reader:sql/root.sql": {Data: assets["reader:sql/root.sql"]}}
	require.NoError(t, Validate(checked, input, output, resources))
	copy := checked.Clone()
	copy.RootView.Columns = nil
	require.ErrorContains(t, Validate(copy, input, output, resources), "checked graph")
	copy = checked.Clone()
	copy.Routes[0].Path = "/foreign"
	require.Error(t, Validate(copy, input, output, resources))
	require.Error(t, Validate(checked, output, input, resources))
	resources["reader:sql/root.sql"] = &fstest.MapFile{Data: []byte("SELECT ID FROM other")}
	require.ErrorContains(t, Validate(checked, input, output, resources), "changed after validation")
	require.Error(t, Validate(checked, input, output, nil))
}

func TestReaderMetadataRejectsIndependentInputViews(t *testing.T) {
	graph := documentFixture().Component
	graph.Views = []*spec.View{{Name: "independent"}}
	err := Contracts(graph, x.NewType(reflect.TypeFor[testInput]()), x.NewType(reflect.TypeFor[testOutput]()), nil)
	require.ErrorContains(t, err, "independent input views are unsupported")
}
