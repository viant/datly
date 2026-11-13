package output

import (
	"bytes"
	"context"
	stdcsv "encoding/csv"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"github.com/xuri/excelize/v2"
)

type scalarExportChild struct {
	State  string `json:"state"`
	Hidden string `json:"hidden"`
}
type scalarExportRow struct {
	State    string             `json:"state" csvName:"Stored state" xls:"name=Stored state"`
	Kind     int                `json:"kind"`
	Code     string             `json:"code"`
	Optional *string            `json:"optional"`
	Hidden   string             `json:"hidden"`
	Child    *scalarExportChild `json:"child"`
	Sibling  *scalarExportChild `json:"sibling"`
}

const aliasExportTag = `choice:"0:Pending" choice:"10:Active" enumStyle:"literal" enumOutputAlias:"0:" enumOutputAlias:"10:ACTIVE_TOKEN"`

func TestScalarSelectionPreservesOtherFormatValues(t *testing.T) {
	type row struct {
		State string `json:"state" xml:"State" choice:"10:Active" enumStyle:"literal" enumOutputAlias:"10:ACTIVE_TOKEN"`
	}
	value := []row{{State: "ACTIVE_TOKEN"}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value)})
	require.NoError(t, err)
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Fields: []string{"State"}}})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(t.Context())
	scoped, finish := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, value, filter)
	finish(value, nil)
	for _, format := range []string{"xml", "tabular"} {
		for _, selected := range []bool{false, true} {
			call := context.Context(t.Context())
			if selected {
				call = ctx
			}
			result, err := plan.Encode(call, format, value)
			require.NoError(t, err)
			require.Contains(t, string(result.Data), "ACTIVE_TOKEN", format)
		}
	}
}

func scalarExportPlan(t testing.TB, reject bool) (*Plan, *Plan) {
	t.Helper()
	typ := reflect.TypeFor[[]scalarExportRow]()
	base, err := (Compiler{}).Compile(CompileInput{Type: typ})
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(path string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		copy := *f
		switch f.Name {
		case "Hidden":
			copy.Tag = reflect.StructTag(`json:"-" ` + aliasExportTag + ` enumOutputUnknown:"reject"`)
		case "State", "Optional":
			// Identical child types carry independent representation metadata.
			if strings.HasSuffix(path, "Sibling") {
				return nil, nil
			}
			copy.Tag = reflect.StructTag(string(f.Tag) + " " + aliasExportTag)
			if reject {
				copy.Tag = reflect.StructTag(string(copy.Tag) + ` enumOutputUnknown:"reject"`)
			}
		case "Kind":
			copy.Tag = `json:"kind" choice:"0:Image" choice:"1:Video" enumStyle:"literal"`
		case "Code":
			copy.Tag = `json:"code" choice:"0:Pending" choice:"10:Active" enumStyle:"ordinal"`
		default:
			return nil, nil
		}
		return &copy, nil
	})
	require.NoError(t, err)
	plan, err := base.WithRepresentation(metadata)
	require.NoError(t, err)
	return base, plan
}
func scalarExportValue() []scalarExportRow {
	token := "ACTIVE_TOKEN"
	return []scalarExportRow{{State: token, Kind: 1, Code: "Active", Optional: &token, Hidden: "PRIVATE", Child: &scalarExportChild{State: token, Hidden: "PRIVATE"}, Sibling: &scalarExportChild{State: token, Hidden: "PRIVATE"}}}
}
func scalarExportRows(t *testing.T, result Result, format string) [][]string {
	t.Helper()
	if format == "csv" {
		rows, err := stdcsv.NewReader(bytes.NewReader(result.Data)).ReadAll()
		require.NoError(t, err)
		return rows
	}
	book, err := excelize.OpenReader(bytes.NewReader(result.Data))
	require.NoError(t, err)
	defer book.Close()
	var rows [][]string
	for _, sheet := range book.GetSheetList() {
		item, err := book.GetRows(sheet)
		require.NoError(t, err)
		rows = append(rows, item...)
	}
	return rows
}
func TestScalarContractExportsUseNativeEnumValues(t *testing.T) {
	base, plan := scalarExportPlan(t, false)
	value := scalarExportValue()
	before, err := json.Marshal(value)
	require.NoError(t, err)
	encoded, err := plan.Encode(t.Context(), "json", value)
	require.NoError(t, err)
	var document []map[string]any
	require.NoError(t, json.Unmarshal(encoded.Data, &document))
	require.Equal(t, "Active", document[0]["state"])
	require.Equal(t, "Video", document[0]["kind"])
	require.Equal(t, float64(10), document[0]["code"])
	require.Equal(t, "ACTIVE_TOKEN", document[0]["sibling"].(map[string]any)["state"])
	projection := plan.formatPresentation
	projected, err := projection.value(reflect.ValueOf(value))
	require.NoError(t, err)
	require.Equal(t, reflect.String, projected.Index(0).FieldByName("Kind").Kind())
	require.Equal(t, reflect.Int64, projected.Index(0).FieldByName("Code").Kind())
	for _, format := range []string{"csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			result, err := plan.Encode(t.Context(), format, value)
			require.NoError(t, err)
			rows := scalarExportRows(t, result, format)
			text := fmtRows(rows)
			if format == "csv" {
				require.Equal(t, []string{"Stored state", "Kind", "Code", "Optional", "Child.State", "Sibling.State"}, rows[0])
				require.Equal(t, []string{"Active", "Video", "10", "Active", "Active", "ACTIVE_TOKEN"}, rows[1])
			} else {
				book, err := excelize.OpenReader(bytes.NewReader(result.Data))
				require.NoError(t, err)
				sheet := book.GetSheetList()[0]
				sheetRows, err := book.GetRows(sheet)
				require.NoError(t, err)
				found := false
				for r, row := range sheetRows {
					for c, value := range row {
						if value != "10" {
							continue
						}
						cell, err := excelize.CoordinatesToCellName(c+1, r+1)
						require.NoError(t, err)
						cellType, err := book.GetCellType(sheet, cell)
						require.NoError(t, err)
						require.Contains(t, []excelize.CellType{excelize.CellTypeNumber, excelize.CellTypeUnset}, cellType)
						found = true
					}
				}
				require.True(t, found, "native workbook must contain a numeric ordinal cell")
				require.NoError(t, book.Close())
			}
			require.Contains(t, text, "Active")
			require.Contains(t, text, "Video")
			require.Contains(t, text, "10")
			require.Contains(t, text, "ACTIVE_TOKEN") // unchanged sibling
			require.Contains(t, text, "Stored state") // native column owner
			require.NotContains(t, text, "PRIVATE")
			require.NotContains(t, text, "Hidden")
			plain, err := base.Encode(t.Context(), format, value)
			require.NoError(t, err)
			require.Contains(t, fmtRows(scalarExportRows(t, plain, format)), "ACTIVE_TOKEN")
		})
	}
	after, err := json.Marshal(value)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))
	require.Same(t, projection, plan.formatPresentation)
}

type scalarCustomJSON string

func (s scalarCustomJSON) MarshalJSON() ([]byte, error) { return json.Marshal("owned:" + string(s)) }

type scalarCustomText string

func (s scalarCustomText) MarshalText() ([]byte, error) { return []byte("owned:" + string(s)), nil }

func TestScalarContractExportsPreserveCustomOwners(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[scalarCustomJSON](), reflect.TypeFor[scalarCustomText]()} {
		projection, err := compileEnumPresentation(reflect.StructField{Name: "State", Type: typ, Tag: aliasExportTag})
		require.Nil(t, projection)
		require.ErrorContains(t, err, "cannot override custom JSON/Text owner")
		projection, err = compileEnumPresentation(reflect.StructField{Name: "State", Type: typ})
		require.NoError(t, err)
		require.Nil(t, projection)
	}
	// Opaque structs remain exact linked types and preserve their private state.
	typ := reflect.TypeFor[json.RawMessage]()
	projection, err := (presentationCompiler{compileEnums: true}).compile(typ, "", nil)
	require.NoError(t, err)
	require.Nil(t, projection)
}
func fmtRows(rows [][]string) string {
	var result string
	for _, row := range rows {
		result += strings.Join(row, "|") + "\n"
	}
	return result
}

func TestScalarContractExportNullEmptyUnknownAndSelection(t *testing.T) {
	_, plan := scalarExportPlan(t, false)
	value := []scalarExportRow{{State: "", Kind: 0, Code: "Pending"}, {State: "UNLISTED", Kind: 1, Code: "Active"}}
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := plan.Encode(t.Context(), format, value)
		require.NoError(t, err)
		text := fmtRows(scalarExportRows(t, result, format))
		require.Contains(t, text, "Pending")
		require.Contains(t, text, "UNLISTED")
		_, err = plan.Encode(t.Context(), format, []scalarExportRow(nil))
		require.NoError(t, err)
		_, err = plan.Encode(t.Context(), format, []scalarExportRow{})
		require.NoError(t, err)
	}
	_, strict := scalarExportPlan(t, true)
	value = scalarExportValue()
	value[0].State = "UNLISTED"
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		_, err := strict.Encode(t.Context(), format, value)
		require.True(t, errors.Is(err, choice.ErrUnknownOutput), "%s: %v", format, err)
	}
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Fields: []string{"Kind"}}})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(context.Background())
	scoped, finish := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, value, filter)
	finish(value, nil)
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		result, err := strict.Encode(ctx, format, value)
		require.NoError(t, err)
		require.NotContains(t, string(result.Data), "UNLISTED")
	}
}
func TestScalarContractExportConcurrentWarmReuse(t *testing.T) {
	_, plan := scalarExportPlan(t, false)
	value := scalarExportValue()
	projection, formatPlan := plan.formatPresentation, plan.formatPlan
	leaf := projection.elem.byIndex[0]
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 10; j++ {
				_, err := plan.Encode(context.Background(), "csv", value)
				if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	workers.Wait()
	require.Same(t, projection, plan.formatPresentation)
	require.Same(t, formatPlan, plan.formatPlan)
	require.Same(t, leaf.enumeration, plan.formatPresentation.elem.byIndex[0].enumeration)
}
func BenchmarkScalarContractExportFullPath(b *testing.B) {
	base, plan := scalarExportPlan(b, false)
	value := make([]scalarExportRow, 100)
	for i := range value {
		value[i] = scalarExportValue()[0]
	}
	for _, format := range []string{"csv", "xlsx"} {
		for _, variant := range []struct {
			name string
			plan *Plan
		}{{"native", base}, {"contract", plan}} {
			b.Run(format+"/"+variant.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := variant.plan.Encode(context.Background(), format, value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	b.Run("compileContract", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := base.WithRepresentation(plan.representation); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type ScalarExportEmbedded struct {
	State string `json:"state"`
	Other string `json:"other"`
}

func TestScalarContractEmbeddedSelectionKeepsCanonicalIndexes(t *testing.T) {
	type row struct {
		*ScalarExportEmbedded
		Sibling ScalarExportEmbedded `json:"sibling"`
		Hidden  string               `json:"hidden"`
	}
	typ := reflect.TypeFor[[]row]()
	base, err := (Compiler{}).Compile(CompileInput{Type: typ})
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(path string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		copy := *f
		if f.Name == "Hidden" {
			copy.Tag = `json:"-"`
			return &copy, nil
		}
		if f.Name == "State" && strings.HasSuffix(path, "ScalarExportEmbedded") {
			copy.Tag = reflect.StructTag(string(f.Tag) + " " + aliasExportTag)
			return &copy, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	plan, err := base.WithRepresentation(metadata)
	require.NoError(t, err)
	value := []row{{ScalarExportEmbedded: &ScalarExportEmbedded{State: "ACTIVE_TOKEN", Other: "unselected"}, Sibling: ScalarExportEmbedded{State: "ACTIVE_TOKEN", Other: "sibling"}, Hidden: "PRIVATE"}}
	filter, err := structjson.NewFieldFilter(typ, []structjson.FieldSelection{{Fields: []string{"State", "Sibling"}, Indexes: [][]int{{0, 0}}}})
	require.NoError(t, err)
	selected, projected, err := plan.selectedPresentation(value, filter, plan.formatPresentation)
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		result, err := selected.Encode(t.Context(), format, projected)
		require.NoError(t, err)
		text := fmtRows(scalarExportRows(t, result, format))
		require.Contains(t, text, "Active")
		require.Contains(t, text, "ACTIVE_TOKEN")
		require.NotContains(t, text, "unselected")
		require.NotContains(t, text, "PRIVATE")
	}
}

func TestScalarExportsUnchangedMetadataAndRESTStorage(t *testing.T) {
	type row struct {
		State    string `json:"state" choice:"0:Pending" choice:"10:Active" enumStyle:"literal" enumOutputAlias:"10:ACTIVE_TOKEN"`
		Kind     int    `json:"kind" choice:"0:Image" choice:"1:Video" enumStyle:"literal"`
		Hidden   string `json:"hidden"`
		Internal string `json:"-"`
	}
	typ := reflect.TypeFor[[]row]()
	base, err := (Compiler{}).Compile(CompileInput{Type: typ})
	require.NoError(t, err)
	require.NotNil(t, base.formatPresentation)
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	unchanged, err := walk.RewriteRepresentation(nil)
	require.NoError(t, err)
	require.False(t, unchanged.Changed())
	unrestricted, err := base.WithRepresentation(unchanged)
	require.NoError(t, err)
	restrictedMetadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name != "Hidden" {
			return nil, nil
		}
		copy := *f
		copy.Tag = `json:"-"`
		return &copy, nil
	})
	require.NoError(t, err)
	restricted, err := base.WithRepresentation(restrictedMetadata)
	require.NoError(t, err)
	restMetadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		field := &xcontract.Field{Name: f.Name, Tag: f.Tag}
		for _, key := range []string{"choice", "enumStyle", "enumOutputAlias"} {
			field = field.RemoveTag(key)
		}
		copy := *f
		copy.Tag = field.Tag
		return &copy, nil
	})
	require.NoError(t, err)
	rest, err := base.WithRepresentation(restMetadata)
	require.NoError(t, err)
	value := []row{{State: "ACTIVE_TOKEN", Kind: 1, Hidden: "PRIVATE", Internal: "INTERNAL_PRIVATE"}}
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		for _, plan := range []*Plan{unrestricted, restricted} {
			result, err := plan.Encode(t.Context(), format, value)
			require.NoError(t, err)
			text := string(result.Data)
			if format != "json" {
				text = fmtRows(scalarExportRows(t, result, format))
			}
			require.Contains(t, text, "Active")
			require.Contains(t, text, "Video")
			require.NotContains(t, text, "ACTIVE_TOKEN")
			require.NotContains(t, text, "INTERNAL_PRIVATE")
		}
		result, err := rest.Encode(t.Context(), format, value)
		require.NoError(t, err)
		text := string(result.Data)
		if format != "json" {
			text = fmtRows(scalarExportRows(t, result, format))
		}
		require.Contains(t, text, "ACTIVE_TOKEN")
		require.NotContains(t, text, "Video")
	}
	plain, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[[]scalarExportChild]()})
	require.NoError(t, err)
	require.Nil(t, plain.formatPresentation)
	require.Nil(t, plain.formatPlan)
}

func TestScalarExportIntegerCarrierKeepsExactPrecision(t *testing.T) {
	type row struct {
		Wide     uint64 `choice:"18446744073709551615:Maximum" enumStyle:"ordinal"`
		Code     string `choice:"18446744073709551615:Maximum" enumStyle:"ordinal"`
		Optional *int64 `choice:"9007199254740993:Wide" enumStyle:"ordinal"`
	}
	number := int64(9007199254740993)
	value := []row{{Wide: ^uint64(0), Code: "Maximum", Optional: &number}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value)})
	require.NoError(t, err)
	projected, err := plan.formatPresentation.value(reflect.ValueOf(value))
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), projected.Index(0).FieldByName("Wide").Uint())
	require.Equal(t, ^uint64(0), projected.Index(0).FieldByName("Code").Uint())
	require.Equal(t, number, projected.Index(0).FieldByName("Optional").Elem().Int())
	result, err := plan.Encode(t.Context(), "csv", value)
	require.NoError(t, err)
	rows := scalarExportRows(t, result, "csv")
	require.Equal(t, []string{"18446744073709551615", "18446744073709551615", "9007199254740993"}, rows[1])
}
