package collector

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	sqlio "github.com/viant/sqlx/io"
	"reflect"
	"testing"
)

func TestUnmappedNullableSQLKeyPreservesNullAndZero(t *testing.T) {
	type row struct{ ID int }
	view := newTestView(&data.View{}, reflect.TypeFor[row]())
	var rows []row
	collector := NewCollector(view, &rows, false)
	resolve := collector.Resolve(sqlio.NewColumn("hidden", "TEXT", reflect.TypeFor[string](), sqlio.WithColumnNullable(true)))
	for _, value := range []any{nil, "", "value", int64(0)} {
		*resolve(nil).(*any) = value
	}
	for i, value := range []any{nil, "", "value", int64(0)} {
		got, ok := collector.sqlKeyAt("hidden", i)
		require.True(t, ok)
		require.Equal(t, sqlio.NormalizeKey(value), got)
	}
}
