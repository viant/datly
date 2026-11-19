package view

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
)

func TestWithSummary(t *testing.T) {
	aView := NewView("vendor", "")
	err := WithSummary(&TemplateSummary{
		Name:      "Meta",
		SourceURL: "vendor/vendor_summary.sql",
	})(aView)
	require.NoError(t, err)
	require.NotNil(t, aView.Template)
	require.NotNil(t, aView.Template.Summary)
	require.Equal(t, "Meta", aView.Template.Summary.Name)
	require.Equal(t, "vendor/vendor_summary.sql", aView.Template.Summary.SourceURL)
}

func TestWithSummaryURI(t *testing.T) {
	aView := NewView("vendor", "")
	err := WithSummaryURI("vendor/vendor_summary.sql")(aView)
	require.NoError(t, err)
	require.NotNil(t, aView.Template)
	require.NotNil(t, aView.Template.Summary)
	require.Equal(t, "vendor/vendor_summary.sql", aView.Template.Summary.SourceURL)
}

func TestWithTemplateParameterStateType(t *testing.T) {
	type input struct {
		Foos *struct {
			ID int
		}
	}

	resource := EmptyResource()
	aView := &View{
		Name:      "foos",
		Table:     "FOOS",
		Schema:    state.NewSchema(reflect.TypeOf(&input{})),
		_resource: resource,
	}
	aView.Template = NewTemplate(
		`$CurFoosId.Values`,
		WithTemplateParameters(
			&state.Parameter{
				Name:   "Foos",
				In:     state.NewBodyLocation(""),
				Schema: state.NewSchema(reflect.TypeOf(&struct{ ID int }{})),
				Tag:    `anonymous:"true"`,
			},
			&state.Parameter{
				Name:   "CurFoosId",
				In:     state.NewParameterLocation("Foos"),
				Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
			},
		),
	)
	require.NoError(t, WithTemplateParameterStateType(true)(aView))
	require.NoError(t, aView.Template.Init(context.Background(), resource, aView))
	require.NotNil(t, aView.Template.StateType())
	require.NotNil(t, aView.Template.StateType().Lookup("Foos"))
	require.NotNil(t, aView.Template.StateType().Lookup("CurFoosId"))
}

func TestWithDeclaredTemplateParametersOnly(t *testing.T) {
	aView := NewView("vendor", "")
	require.NoError(t, WithDeclaredTemplateParametersOnly(true)(aView))
	require.NotNil(t, aView.Template)
	require.True(t, aView.Template.DeclaredParametersOnly)
}

func TestWithResourceParameterLookup(t *testing.T) {
	aView := NewView("vendor", "")
	require.NoError(t, WithResourceParameterLookup(true)(aView))
	require.NotNil(t, aView.Template)
	require.True(t, aView.Template.UseResourceParameterLookup)
}
