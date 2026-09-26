package transcribe

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/generate"
)

func TestGeneratedSelectorPolicyPrecedence(t *testing.T) {
	for _, policy := range []string{"unspecified", "true", "false"} {
		t.Run(policy, func(t *testing.T) {
			directives := ""
			if policy != "unspecified" {
				for _, directive := range []string{"fields", "order_by", "criteria", "limit", "offset", "page"} {
					directives += fmt.Sprintf(",selector_%s(site,%s)", directive, policy)
				}
			}
			compiled, err := NewCompiler().Compile(context.Background(), &Source{
				Name: "Site", Scope: "example.com/site", Text: `#setting($_ = $route('/site','GET'))
#set($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('site'))
#set($_ = $OrderBy<string>(query/si_orderby).Optional().QuerySelector('site'))
#set($_ = $Criteria<string>(query/criteria).Optional().QuerySelector('site'))
#set($_ = $Limit<int>(query/limit).Optional().QuerySelector('site'))
#set($_ = $Offset<int>(query/offset).Optional().QuerySelector('site'))
#set($_ = $Page<int>(query/page).Optional().QuerySelector('site'))
#set($_ = $Data<?>(output/view))
SELECT site.id` + directives + ` FROM sites site`,
			})
			require.NoError(t, err)
			want := policy != "false"
			check := func(selector *spec.Selector) {
				t.Helper()
				require.NotNil(t, selector)
				require.Equal(t, []bool{want, want, want, want, want, want}, []bool{selector.AllowFields, selector.AllowOrderBy, selector.AllowCriteria, selector.AllowLimit, selector.AllowOffset, selector.AllowPage})
			}
			check(compiled.Component.RootView.Selector)
			generator := generate.New(generate.Input{Component: compiled.Component})
			dir := t.TempDir()
			result, err := generator.Generate(dir)
			require.NoError(t, err)
			regenerated, err := generator.Generate(dir)
			require.NoError(t, err)
			require.Equal(t, result.Files, regenerated.Files)
			for _, field := range result.Plan.Output.Fields {
				if field.Name == "Data" {
					for _, key := range []string{"selectorProjection", "selectorOrderBy", "selectorCriteria", "selectorLimit", "selectorOffset", "selectorPage"} {
						require.Contains(t, field.Tag, fmt.Sprintf("%s=%v", key, want))
					}
				}
			}
			inputType, err := generator.RuntimeInputType()
			require.NoError(t, err)
			outputType, err := generator.RuntimeOutputType()
			require.NoError(t, err)
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: inputType, OutputType: outputType, DirectViewField: "Data"})
			require.NoError(t, err)
			check(artifact.Reader.Root.View.Spec.Selector)
		})
	}
}

func TestGeneratedCodecSelectorExplicitDeny(t *testing.T) {
	compiled, err := NewCompiler().Compile(context.Background(), &Source{Name: "Site", Scope: "example.com/site", Text: `#setting($_ = $route('/site','GET'))
#set($_ = $OrderBy<[]string,string>(query/si_orderby).Optional().WithCodec('OrderByList').QuerySelector('site'))
#set($_ = $Page<int>(query/si_page).Optional().QuerySelector('site'))
#set($_ = $Data<?>(output/view))
SELECT site.id, selector_order_by(site,false), selector_page(site,false) FROM sites site`})
	require.NoError(t, err)
	plan, err := generate.New(generate.Input{Component: compiled.Component}).Plan()
	require.NoError(t, err)
	require.False(t, compiled.Component.RootView.Selector.AllowOrderBy)
	require.False(t, compiled.Component.RootView.Selector.AllowPage)
	require.Contains(t, plan.Input.Fields[0].Tag, `codec:"OrderByList`)
	var dataTag string
	for _, field := range plan.Output.Fields {
		if field.Name == "Data" {
			dataTag = field.Tag
		}
	}
	require.Contains(t, dataTag, "selectorOrderBy=false")
	require.Contains(t, dataTag, "selectorPage=false")
}

func TestAllowedOrderColumnsCannotOverrideExplicitDeny(t *testing.T) {
	for _, directives := range []string{
		`selector_order_by(site,false), allowed_order_by_columns(site,'id')`,
		`allowed_order_by_columns(site,'id'), selector_order_by(site,false)`,
	} {
		compiled, err := NewCompiler().Compile(context.Background(), &Source{Name: "Site", Text: `#setting($_ = $route('/site','GET'))
SELECT site.id, ` + directives + ` FROM sites site`})
		require.NoError(t, err)
		require.False(t, compiled.Component.RootView.Selector.AllowOrderBy)
	}
}

func TestContractLinkerNormalizesInferredSelectorPolicy(t *testing.T) {
	authored := &spec.Component{Name: "Records", RootView: &spec.View{Name: "rows"}, Parameters: []*spec.Parameter{{Name: "OrderBy", QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyOrderBy}}}}
	compiled := authored.Clone()
	require.NoError(t, compiled.RootView.EnableQuerySelector(spec.SelectorPropertyOrderBy))
	linker := &contractLinker{packageComponent: authored, compiledComponent: compiled}
	require.True(t, linker.equalViews())
	require.Nil(t, authored.RootView.Selector, "comparison must not mutate authored metadata")
	require.NoError(t, compiled.RootView.Selector.SetPermission(spec.SelectorPropertyOrderBy, false))
	require.False(t, linker.equalViews(), "an authored permission change must remain a contract change")
}
