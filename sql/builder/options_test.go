package builder

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/metadata/info"
	xstate "github.com/viant/xdatly/state"
)

func TestBuilderOptions_Apply(t *testing.T) {
	component := &spec.Component{Name: "Users"}
	controls := &spec.ViewControls{OrderBy: "id DESC"}
	selector := &xstate.Selector{Columns: []string{"id"}}
	input := reflect.ValueOf(struct{ ID int }{ID: 7})
	relation := &data.Relation{}
	dialect := &info.Dialect{}

	options := newBuilderOptions(
		WithBuilderSQL("SELECT id FROM users"),
		WithBuilderComponent(component),
		WithBuilderControls(controls),
		WithBuilderSelector(selector),
		WithBuilderInput(input),
		WithBuilderRelation(relation),
		WithBuilderDialect(dialect),
		WithBuilderPositionalArgs([]any{1, 2}),
		WithBuilderCompositeArgs([]string{"tenant_id", "user_id"}, [][]interface{}{{1, 7}}),
		WithBuilderMatcher("user_id", []any{7}),
		WithBuilderProjection([]string{"id"}),
		WithBuilderSkipRelationFilter(true),
	)

	if options.sqlText != "SELECT id FROM users" || options.component != component || options.controls != controls || options.selector != selector {
		t.Fatalf("unexpected option assignment: %+v", options)
	}
	if !options.input.IsValid() || options.input.FieldByName("ID").Int() != 7 {
		t.Fatalf("unexpected input assignment: %+v", options.input)
	}
	if options.relation != relation || options.dialect != dialect || options.matcherBy != "user_id" || !options.skipRelationFilter {
		t.Fatalf("unexpected relation/dialect assignment: %+v", options)
	}
	if len(options.positionalArgs) != 2 || len(options.compositeColumns) != 2 || len(options.compositeRows) != 1 || len(options.matcherIn) != 1 || len(options.projection) != 1 {
		t.Fatalf("unexpected slice assignment: %+v", options)
	}
}
