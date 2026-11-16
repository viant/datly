package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPreparedSelectorSourceRejectsChangedLineageAndDeclarations(t *testing.T) {
	pkg := reflect.TypeFor[fixture.Component]().PkgPath()
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	require.NoError(t, err)
	text := `#package('selectorcompanion')
#setting($_ = $route('/selector-records','GET'))
#setting($_ = $input_type('` + pkg + `.Input'))
#setting($_ = $output_type('` + pkg + `.Output'))
#define($_ = $ID<int>(query/id).Optional())
SELECT r.* FROM (SELECT id,label FROM records) r`
	source := &Source{Scope: pkg, Name: "ReadRecords", Text: text, Path: filepath.Join(t.TempDir(), "ReadRecords.dql"), PreparedColumns: func(context.Context, column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
		return &column.PreparedColumnResult{Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}}, {Name: "label", Source: "label", Type: spec.TypeRef{Name: "string"}}}}, nil
	}}
	result, err := (&PackageCompilation{Source: source, Component: found.Components[0], InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Types: found.Types}).Compile(context.Background())
	require.NoError(t, err)
	evidence, err := preparedSelectorSource(result)
	require.NoError(t, err)
	require.NotNil(t, evidence)
	require.NoError(t, evidence.Validate(result.Component))
	for _, tc := range []struct {
		name   string
		mutate func(*Result)
	}{
		{"text", func(r *Result) { r.Source.Text += "\n-- changed" }},
		{"path", func(r *Result) { r.Source.Path += ".foreign" }},
		{"resource lineage", func(r *Result) { r.Source.Resources = resource.New() }},
		{"missing preparation", func(r *Result) { r.Source.preparation = nil }},
		{"declaration", func(r *Result) {
			for _, p := range r.Component.Parameters {
				if p.Name == "ID" {
					value := true
					p.Required = &value
				}
			}
		}},
		{"copied association", func(r *Result) {
			a := r.Component.SelectorCompanion
			h, e := a.Holder()
			require.NoError(t, e)
			c, e := a.Companion()
			require.NoError(t, e)
			r.Component.SelectorCompanion, e = selectorModel.New(h, a.Field(), c)
			require.NoError(t, e)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := *result
			source := *result.Source
			changed.Source = &source
			changed.Component = result.Component.Clone()
			tc.mutate(&changed)
			_, err := preparedSelectorSource(&changed)
			require.Error(t, err)
		})
	}
	changed := result.Component.Clone()
	changed.Parameters[0].Description = "forged"
	require.Error(t, evidence.Validate(changed))
}
