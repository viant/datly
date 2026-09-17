package reader

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/collector"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

func TestScannedRelationKeyCopiesValues(t *testing.T) {
	number, text, flag := 99, "", false
	pointer := &number
	bytes := []byte("abc")
	for _, tc := range []struct {
		name        string
		input, want any
	}{
		{"int", &number, 99}, {"pointer", &pointer, 99}, {"string", &text, ""}, {"bool", &flag, false},
		{"bytes", &bytes, "abc"}, {"nil", (*int)(nil), nil}, {"null", &sql.NullInt64{}, nil},
		{"nullable", &sql.NullInt64{Int64: 7, Valid: true}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := scannedRelationKey(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, actual)
		})
	}
	actual, err := scannedRelationKey(&pointer)
	require.NoError(t, err)
	number = 0
	require.Equal(t, 99, actual)
	actual, err = scannedRelationKey(&bytes)
	require.NoError(t, err)
	bytes[0] = 'z'
	require.Equal(t, "abc", actual)
}

func TestMappedSQLKeyCaptureRollsBackSkippedRows(t *testing.T) {
	type child struct{ ID int }
	type row struct {
		ID       int      `sqlx:"id"`
		Raw      int      `sqlx:"-"`
		Children []*child `view:"children" on:"Raw:id=ID:id" sql:"SELECT id FROM children WHERE $COLUMN_IN"`
	}
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2),(3),(4)"))
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records ORDER BY id"}}}
	root := data.FromComponent(component)
	childView := &data.View{Spec: spec.View{Name: "children"}}
	root.Relations = []*data.Relation{{Name: "children", Holder: "Children", Kind: spec.RelationKindSubview, Cardinality: spec.CardinalityMany,
		On: data.Links{data.NewLink("", "id", "Raw")},
		Of: &data.RelationRef{View: childView, On: data.Links{data.NewLink("", "id", "ID")}}}}
	graph, err := collector.Compile(root, map[*data.View]reflect.Type{root: reflect.TypeFor[*row](), childView: reflect.TypeFor[*child]()})
	require.NoError(t, err)
	var rows []*row
	col := collector.NewCollector(graph.Root, &rows, false)
	capture := &relationKeyCapture{collector: col, columns: col.SQLKeyColumns()}
	query, err := sqlxread.New(ctx, db.DB, component.RootView.Source.SQL, col.NewItem(),
		sqlxread.WithUnmappedFn(unmappedResolver(col)), sqlxread.WithRowMapper(capture.mapper),
		sqlxread.WithInMatcher(&cache.ParmetrizedQuery{By: "id", In: []any{2, 3}, OnSkip: col.OnSkip}))
	require.NoError(t, err)
	t.Cleanup(func() {
		if query.Stmt() != nil {
			_ = query.Stmt().Close()
		}
	})
	visitor := col.Visitor(ctx)
	require.NoError(t, query.QueryAll(ctx, func(value any) error {
		if err := capture.snapshot(); err != nil {
			return err
		}
		value.(*row).ID += 100
		return visitor(value)
	}))
	col.Fetched()
	require.Equal(t, []*row{{ID: 102}, {ID: 103}}, rows)
	children := col.Relations(nil)
	require.Len(t, children, 1)
	keys, _, _ := children[0].ParentPlaceholders()
	require.Equal(t, []any{2, 3}, keys)
}
