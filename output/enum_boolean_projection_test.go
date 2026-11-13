package output

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"github.com/xuri/excelize/v2"
)

type booleanExportRow struct {
	Flag     int  `json:"flag" mcpEnum:"true" choice:"0:false" choice:"1:true" enumAccept:"boolean" enumStyle:"boolean"`
	Optional *int `json:"optional" mcpEnum:"true" choice:"0:false" choice:"1:true" enumAccept:"boolean" enumStyle:"boolean"`
	Hidden   int  `json:"-" mcpEnum:"true" choice:"0:false" choice:"1:true" enumAccept:"boolean" enumStyle:"boolean"`
}

func TestBooleanEnumPresentationAcrossFormats(t *testing.T) {
	zero := 0
	value := []booleanExportRow{{Flag: 0, Optional: &zero, Hidden: 2}, {Flag: 1, Hidden: 2}}
	typ := reflect.TypeOf(value)
	base, err := (Compiler{}).Compile(CompileInput{Type: typ})
	require.NoError(t, err)
	plan, err := base.WithMCPVisibility()
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	storage, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Tag.Get("mcpEnum") != "true" {
			return nil, nil
		}
		field := (&xcontract.Field{Name: f.Name, Tag: f.Tag}).RemoveTag("choice").RemoveTag("enumAccept").RemoveTag("enumStyle")
		copy := *f
		copy.Tag = field.Tag
		return &copy, nil
	})
	require.NoError(t, err)
	rest, err := base.WithRepresentation(storage)
	require.NoError(t, err)
	before, err := json.Marshal(value)
	require.NoError(t, err)
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			encoded, err := plan.Encode(t.Context(), format, value)
			require.NoError(t, err)
			stored, err := rest.Encode(t.Context(), format, value)
			require.NoError(t, err)
			if format == "json" {
				require.JSONEq(t, `[{"flag":false,"optional":false},{"flag":true,"optional":null}]`, string(encoded.Data))
				require.JSONEq(t, `[{"flag":0,"optional":0},{"flag":1,"optional":null}]`, string(stored.Data))
			} else {
				rows := scalarExportRows(t, encoded, format)
				require.Equal(t, []string{"Flag", "Optional"}, rows[0])
				if format == "csv" {
					require.Equal(t, [][]string{{"Flag", "Optional"}, {"false", "false"}, {"true", "null"}}, rows)
				} else {
					book, err := excelize.OpenReader(bytes.NewReader(encoded.Data))
					require.NoError(t, err)
					sheet := book.GetSheetList()[0]
					for _, cell := range []string{"A2", "B2", "A3"} {
						kind, err := book.GetCellType(sheet, cell)
						require.NoError(t, err)
						require.Equal(t, excelize.CellTypeBool, kind, cell)
					}
					flag, err := book.GetCellValue(sheet, "A2")
					require.NoError(t, err)
					require.Equal(t, "FALSE", flag)
					flag, err = book.GetCellValue(sheet, "A3")
					require.NoError(t, err)
					require.Equal(t, "TRUE", flag)
					require.NoError(t, book.Close())
				}
				storageRows := scalarExportRows(t, stored, format)
				require.Equal(t, "0", storageRows[1][0])
				require.Equal(t, "1", storageRows[2][0])
			}
			filter, err := structjson.NewFieldFilter(typ, []structjson.FieldSelection{{Fields: []string{"Flag"}}})
			require.NoError(t, err)
			ctx := dexec.CaptureOutputSelection(t.Context())
			scoped, finish := dexec.ScopeOutputSelection(ctx)
			dexec.BeginOutputSelection(scoped)
			dexec.PublishOutputSelection(scoped, value, filter)
			finish(value, nil)
			selected, err := plan.Encode(ctx, format, value)
			require.NoError(t, err)
			if format == "json" {
				require.JSONEq(t, `[{"flag":false},{"flag":true}]`, string(selected.Data))
			} else {
				require.Equal(t, []string{"Flag"}, scalarExportRows(t, selected, format)[0])
			}
		})
	}
	after, err := json.Marshal(value)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "wire conversion must preserve source integers and pointers")
	invalid := []booleanExportRow{{Flag: 2}}
	filter, err := structjson.NewFieldFilter(typ, []structjson.FieldSelection{{Fields: []string{"Optional"}}})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(t.Context())
	scoped, finish := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, invalid, filter)
	finish(invalid, nil)
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		_, err := plan.Encode(t.Context(), format, invalid)
		require.ErrorIs(t, err, choice.ErrUnknownOutput, format)
		_, err = plan.Encode(ctx, format, invalid)
		require.NoError(t, err, "unselected invalid fields must not be encoded: %s", format)
	}
}
