package reader

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
	sqlxread "github.com/viant/sqlx/io/read"
)

func TestSQLXReader_AnonymousVsNamedPatchType(t *testing.T) {
	if os.Getenv("TEST") != "1" {
		t.Skip("set TEST=1 to run integration reader check")
	}

	db, err := sql.Open("mysql", "root:dev@tcp(localhost:3306)/dev?parseTime=true")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("anonymous", func(t *testing.T) {
		type anonymousHas struct {
			Id       bool
			Name     bool
			Quantity bool
		}
		type anonymousRow struct {
			Id       int
			Name     *string
			Quantity *int
			Has      *anonymousHas
		}

		reader, err := sqlxread.New(context.Background(), db, "SELECT * FROM FOOS WHERE ID = 4", func() interface{} {
			return &anonymousRow{}
		})
		require.NoError(t, err)

		var rows []*anonymousRow
		err = reader.QueryAll(context.Background(), func(row interface{}) error {
			rows = append(rows, row.(*anonymousRow))
			return nil
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, 4, rows[0].Id)
	})

	t.Run("named-reflect-structof", func(t *testing.T) {
		hasType := reflect.StructOf([]reflect.StructField{
			{Name: "Id", Type: reflect.TypeOf(true)},
			{Name: "Name", Type: reflect.TypeOf(true)},
			{Name: "Quantity", Type: reflect.TypeOf(true)},
		})
		rowType := reflect.StructOf([]reflect.StructField{
			{Name: "Id", Type: reflect.TypeOf(int(0)), Tag: `sqlx:"ID"`},
			{Name: "Name", Type: reflect.TypeOf((*string)(nil)), Tag: `sqlx:"NAME"`},
			{Name: "Quantity", Type: reflect.TypeOf((*int)(nil)), Tag: `sqlx:"QUANTITY"`},
			{Name: "Has", Type: reflect.PtrTo(hasType), Tag: `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-"`},
		})

		reader, err := sqlxread.New(context.Background(), db, "SELECT * FROM FOOS WHERE ID = 4", func() interface{} {
			return reflect.New(rowType).Interface()
		})
		require.NoError(t, err)

		var rows []interface{}
		err = reader.QueryAll(context.Background(), func(row interface{}) error {
			rows = append(rows, row)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
	})

	t.Run("collector-backed-anonymous", func(t *testing.T) {
		type anonymousHas struct {
			Id       bool
			Name     bool
			Quantity bool
		}
		type anonymousRow struct {
			Id       int           `sqlx:"ID"`
			Name     *string       `sqlx:"NAME"`
			Quantity *int          `sqlx:"QUANTITY"`
			Has      *anonymousHas `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-"`
		}

		aView := &view.View{
			Name:   "CurFoos",
			Schema: vstate.NewSchema(reflect.TypeOf([]*anonymousRow{})),
		}
		aView.Schema.Cardinality = vstate.Many
		collector := view.NewCollector(aView.Schema.Slice(), aView, &[]*anonymousRow{}, nil, false)
		reader, err := sqlxread.New(context.Background(), db, "SELECT * FROM FOOS WHERE ID = 4", collector.NewItem())
		require.NoError(t, err)

		err = reader.QueryAll(context.Background(), collector.Visitor(context.Background()))
		require.NoError(t, err)
		dest := collector.Dest().([]*anonymousRow)
		require.Len(t, dest, 1)
		require.Equal(t, 4, dest[0].Id)
	})

	t.Run("collector-backed-reflect-structof", func(t *testing.T) {
		hasType := reflect.StructOf([]reflect.StructField{
			{Name: "Id", Type: reflect.TypeOf(true)},
			{Name: "Name", Type: reflect.TypeOf(true)},
			{Name: "Quantity", Type: reflect.TypeOf(true)},
		})
		rowType := reflect.StructOf([]reflect.StructField{
			{Name: "Id", Type: reflect.TypeOf(int(0)), Tag: `sqlx:"ID"`},
			{Name: "Name", Type: reflect.TypeOf((*string)(nil)), Tag: `sqlx:"NAME"`},
			{Name: "Quantity", Type: reflect.TypeOf((*int)(nil)), Tag: `sqlx:"QUANTITY"`},
			{Name: "Has", Type: reflect.PtrTo(hasType), Tag: `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-"`},
		})
		sliceType := reflect.SliceOf(reflect.PtrTo(rowType))
		aView := &view.View{
			Name:   "CurFoos",
			Schema: vstate.NewSchema(sliceType),
		}
		aView.Schema.Cardinality = vstate.Many

		destPtr := reflect.New(sliceType).Interface()
		collector := view.NewCollector(aView.Schema.Slice(), aView, destPtr, nil, false)
		reader, err := sqlxread.New(context.Background(), db, "SELECT * FROM FOOS WHERE ID = 4", collector.NewItem())
		require.NoError(t, err)

		err = reader.QueryAll(context.Background(), collector.Visitor(context.Background()))
		require.NoError(t, err)
		destValue := reflect.ValueOf(collector.Dest())
		require.Equal(t, 1, destValue.Len())
		require.Equal(t, int64(4), destValue.Index(0).Elem().FieldByName("Id").Int())
	})

	t.Run("collector-backed-reflect-structof-v1-order", func(t *testing.T) {
		hasType := reflect.StructOf([]reflect.StructField{
			{Name: "Name", Type: reflect.TypeOf(true)},
			{Name: "Quantity", Type: reflect.TypeOf(true)},
			{Name: "Id", Type: reflect.TypeOf(true)},
		})
		rowType := reflect.StructOf([]reflect.StructField{
			{Name: "Name", Type: reflect.TypeOf((*string)(nil)), Tag: `sqlx:"NAME"`},
			{Name: "Quantity", Type: reflect.TypeOf((*int)(nil)), Tag: `sqlx:"QUANTITY"`},
			{Name: "Id", Type: reflect.TypeOf(int(0)), Tag: `sqlx:"ID"`},
			{Name: "Has", Type: reflect.PtrTo(hasType), Tag: `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-"`},
		})
		sliceType := reflect.SliceOf(reflect.PtrTo(rowType))
		aView := &view.View{
			Name:   "CurFoos",
			Schema: vstate.NewSchema(sliceType),
		}
		aView.Schema.Cardinality = vstate.Many

		destPtr := reflect.New(sliceType).Interface()
		collector := view.NewCollector(aView.Schema.Slice(), aView, destPtr, nil, false)
		reader, err := sqlxread.New(context.Background(), db, "SELECT * FROM FOOS WHERE ID = 4", collector.NewItem())
		require.NoError(t, err)

		err = reader.QueryAll(context.Background(), collector.Visitor(context.Background()))
		require.NoError(t, err)
		destValue := reflect.ValueOf(collector.Dest())
		require.Equal(t, 1, destValue.Len())
		require.Equal(t, int64(4), destValue.Index(0).Elem().FieldByName("Id").Int())
	})
}
