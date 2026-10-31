package report

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/output"
	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/datly/spec"
)

func TestComposeDirectProjectionPreservesNativeDateTag(t *testing.T) {
	catalog, err := cubecompose.NewCatalog(
		cubecompose.Field{Name: "day", Type: reflect.TypeFor[*time.Time](), Role: cubecompose.Dimension, Tag: `json:"originalDay" format:"dateFormat=YYYY-MM-DD"`},
		cubecompose.Field{Name: "raw", Type: reflect.TypeFor[*time.Time](), Role: cubecompose.Dimension},
	)
	require.NoError(t, err)
	plan, err := cubecompose.Compile("SELECT t1.day AS renamed, t1.raw AS raw FROM $CubeSQL1 t1 LIMIT 5", catalog, 1, 5)
	require.NoError(t, err)
	rowType, err := (&composeHandler{}).rowType(plan)
	require.NoError(t, err)
	dayField, _ := rowType.FieldByName("Renamed")
	require.Equal(t, "dateFormat=YYYY-MM-DD", dayField.Tag.Get("format"))
	require.Equal(t, "renamed", dayField.Tag.Get("json"))
	encoder, err := (output.Compiler{}).Compile(output.CompileInput{Type: rowType, Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	day := time.Date(2026, 9, 1, 23, 30, 0, 0, time.FixedZone("source", -7*3600))
	row := reflect.New(rowType)
	row.Elem().FieldByName("Renamed").Set(reflect.ValueOf(&day))
	row.Elem().FieldByName("Raw").Set(reflect.ValueOf(&day))
	wire, err := encoder.Encode(context.Background(), "json", row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"renamed":"2026-09-01","raw":"2026-09-01T23:30:00-07:00"}`, string(wire.Data))
	require.Equal(t, 23, day.Hour(), "output formatting must not truncate the typed value")
	responseEncoder, err := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeFor[ComposeResponse](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	data := reflect.MakeSlice(reflect.SliceOf(row.Type()), 0, 1)
	data = reflect.Append(data, row)
	responseWire, err := responseEncoder.Encode(context.Background(), "json", &ComposeResponse{Data: data.Interface()})
	require.NoError(t, err)
	require.Contains(t, string(responseWire.Data), `"renamed":"2026-09-01"`)
	require.Contains(t, string(responseWire.Data), `"raw":"2026-09-01T23:30:00-07:00"`)
	row.Elem().FieldByName("Renamed").SetZero()
	wire, err = encoder.Encode(context.Background(), "json", row.Interface())
	require.NoError(t, err)
	require.JSONEq(t, `{"renamed":null,"raw":"2026-09-01T23:30:00-07:00"}`, string(wire.Data))
	computed, err := cubecompose.Compile("SELECT COUNT(t1.day) AS count FROM $CubeSQL1 t1 LIMIT 5", catalog, 1, 5)
	require.NoError(t, err)
	require.Empty(t, computed.Columns[0].RuntimeTag(), "calculated values must not inherit unrelated date formatting")
	for _, tc := range []struct{ first, second, format string }{
		{"t1.day", "t2.day", "dateFormat=YYYY-MM-DD"},
		{"t1.day", "t2.raw", ""},
		{"t1.raw", "t2.day", ""},
		{"t1.day", "NULL", "dateFormat=YYYY-MM-DD"},
		{"NULL", "t2.day", "dateFormat=YYYY-MM-DD"},
	} {
		union, err := cubecompose.Compile("SELECT "+tc.first+" AS day,t1.raw AS raw FROM $CubeSQL1 t1 UNION ALL SELECT "+tc.second+" AS day,t2.raw AS raw FROM $CubeSQL2 t2 LIMIT 5", catalog, 2, 5)
		require.NoError(t, err)
		require.Equal(t, tc.format, union.Columns[0].RuntimeTag().Get("format"), tc.first+" / "+tc.second)
	}
}
