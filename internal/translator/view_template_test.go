package translator

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
)

func TestViewBuildTemplate_UsesSanitizedSQLAndRetainsPredicateParameters(t *testing.T) {
	// Regression coverage for the translator change that must keep WHERE/HAVING
	// predicate builders discoverable without regressing SQL-fragment placeholders to '?' binds.
	rawSQL := `SELECT opaque_root.*
FROM ($table) opaque_root
${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}
GROUP BY opaque_root.id
${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}
WHERE opaque_root.dstamp BETWEEN $From AND $To`

	sanitizedSQL := `SELECT opaque_root.*
FROM ($Unsafe.table) opaque_root
${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}
GROUP BY opaque_root.id
${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}
WHERE opaque_root.dstamp BETWEEN $criteria.AppendBinding($Unsafe.From) AND $criteria.AppendBinding($Unsafe.To)`

	resourceState := inference.State{
		inference.NewConstParameter("table", "ci_event.audience_event_v1"),
		{
			Parameter: state.Parameter{
				Name:   "From",
				In:     state.NewQueryLocation("from"),
				Schema: state.NewSchema(reflect.TypeOf("")),
			},
		},
		{
			Parameter: state.Parameter{
				Name:   "To",
				In:     state.NewQueryLocation("to"),
				Schema: state.NewSchema(reflect.TypeOf("")),
			},
		},
		{
			Parameter: state.Parameter{
				Name:       "Cutoff",
				In:         state.NewQueryLocation("cutoff"),
				Schema:     state.NewSchema(reflect.TypeOf("")),
				Predicates: []*extension.PredicateConfig{{Group: 0, Name: "greater_or_equal"}},
			},
		},
		{
			Parameter: state.Parameter{
				Name:       "Threshold",
				In:         state.NewQueryLocation("threshold"),
				Schema:     state.NewSchema(reflect.TypeOf(0)),
				Predicates: []*extension.PredicateConfig{{Group: 1, Name: "expr"}},
			},
		},
	}

	namespace := &Viewlet{
		Name:         "opaque_root",
		SQL:          rawSQL,
		SanitizedSQL: sanitizedSQL,
		Resource: &Resource{
			State: resourceState,
		},
	}
	rule := &Rule{Root: "opaque_root"}
	subject := &View{
		Namespace: "opaque_root",
		View:      View{}.View,
	}
	subject.Name = "opaque_root"

	subject.buildTemplate(namespace, rule)

	assert.Equal(t, sanitizedSQL, subject.Template.Source)
	assert.ElementsMatch(t, []string{"table", "From", "To", "Cutoff", "Threshold"}, templateParameterNames(subject.Template.Parameters))
}

func TestViewBuildTemplate_FallsBackToRawSQLWhenSanitizedSQLMissing(t *testing.T) {
	rawSQL := `SELECT * FROM $table WHERE created_at >= $From`

	namespace := &Viewlet{
		Name: "vendor",
		SQL:  rawSQL,
		Resource: &Resource{
			State: inference.State{
				inference.NewConstParameter("table", "ci_ads.vendor"),
				{
					Parameter: state.Parameter{
						Name:   "From",
						In:     state.NewQueryLocation("from"),
						Schema: state.NewSchema(reflect.TypeOf("")),
					},
				},
			},
		},
	}
	rule := &Rule{Root: "vendor"}
	subject := &View{
		Namespace: "vendor",
		View:      View{}.View,
	}
	subject.Name = "vendor"

	subject.buildTemplate(namespace, rule)

	assert.Equal(t, rawSQL, subject.Template.Source)
	assert.ElementsMatch(t, []string{"table", "From"}, templateParameterNames(subject.Template.Parameters))
}

func templateParameterNames(params []*state.Parameter) []string {
	result := make([]string, 0, len(params))
	for _, param := range params {
		name := param.Name
		if name == "" {
			name = param.Ref
		}
		result = append(result, name)
	}
	return result
}
