package output

import (
	"bytes"
	"context"
	dexec "github.com/viant/datly/exec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/xuri/excelize/v2"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

type contractFormatChild struct {
	Visible string `json:"visible"`
	Secret  string `json:"secret"`
}
type contractFormatRow struct {
	ID     int                  `json:"id"`
	Secret string               `json:"secret"`
	Child  *contractFormatChild `json:"child"`
}
type contractFormatOutput struct {
	Data []*contractFormatRow `json:"data"`
}

// Native CSV must consume a typed presentation because its codec does not
// interpret JSON visibility tags.
func TestNativeProjectionPreservesContractCSVVisibility(t *testing.T) {
	original := reflect.TypeFor[contractFormatOutput]()
	rewrite, err := xshape.CompileRewrite(original)
	require.NoError(t, err)
	metadata, err := rewrite.RewriteRepresentation(func(_ string, _ reflect.Type, field *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if field.Name != "Secret" {
			return nil, nil
		}
		copy := *field
		copy.Tag = `json:"-" internal:"true" mcp:"-"`
		return &copy, nil
	})
	require.NoError(t, err)
	source := &contractFormatOutput{Data: []*contractFormatRow{{ID: 7, Secret: "PRIVATE_ROOT", Child: &contractFormatChild{Visible: "public", Secret: "PRIVATE_CHILD"}}}}
	base, err := (Compiler{}).Compile(CompileInput{Type: original, DataField: "Data", Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	plan, err := base.WithRepresentation(metadata)
	require.NoError(t, err)
	encoded, err := plan.Encode(context.Background(), "csv", source)
	require.NoError(t, err)
	require.Contains(t, string(encoded.Data), "public")
	require.Contains(t, string(encoded.Data), "7")
	require.NotContains(t, strings.ToLower(string(encoded.Data)), "secret")
	require.NotContains(t, string(encoded.Data), "PRIVATE")
	require.Equal(t, "PRIVATE_ROOT", source.Data[0].Secret)
	require.Equal(t, "PRIVATE_CHILD", source.Data[0].Child.Secret)
}

func contractExportText(t *testing.T, plan *Plan, ctx context.Context, format string, value any) string {
	t.Helper()
	encoded, err := plan.Encode(ctx, format, value)
	require.NoError(t, err)
	if format == "csv" {
		return string(encoded.Data)
	}
	workbook, err := excelize.OpenReader(bytes.NewReader(encoded.Data))
	require.NoError(t, err)
	defer workbook.Close()
	var text string
	for _, sheet := range workbook.GetSheetList() {
		rows, err := workbook.GetRows(sheet)
		require.NoError(t, err)
		for _, row := range rows {
			text += strings.Join(row, "|") + "\n"
		}
	}
	return text
}
func TestContractExportsComposeSelectionAndVariants(t *testing.T) {
	original := reflect.TypeFor[contractFormatOutput]()
	base, err := (Compiler{}).Compile(CompileInput{Type: original, DataField: "Data"})
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(original)
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name == "Secret" || f.Name == "ID" {
			copy := *f
			copy.Tag = `json:"-"`
			return &copy, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	plan, err := base.WithRepresentation(metadata)
	require.NoError(t, err)
	value := &contractFormatOutput{Data: []*contractFormatRow{{ID: 7, Secret: "PRIVATE", Child: &contractFormatChild{Visible: "public", Secret: "PRIVATE"}}}}
	filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: []string{"Data"}, Fields: []string{"Secret", "Child"}}})
	require.NoError(t, err)
	ctx := dexec.CaptureOutputSelection(context.Background())
	scoped, finish := dexec.ScopeOutputSelection(ctx)
	dexec.BeginOutputSelection(scoped)
	dexec.PublishOutputSelection(scoped, value, filter)
	finish(value, nil)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			require.True(t, plan.SupportsFormat(format))
			_, err := plan.Wire(format)
			require.NoError(t, err)
			for _, selected := range []context.Context{context.Background(), ctx} {
				text := contractExportText(t, plan, selected, format, value)
				require.Contains(t, text, "public")
				require.NotContains(t, strings.ToLower(text), "secret")
				require.NotContains(t, text, "PRIVATE")
			}
			full := contractExportText(t, base, context.Background(), format, value)
			require.Contains(t, full, "PRIVATE")
		})
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := plan.Encode(context.Background(), "csv", value)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	require.Equal(t, "PRIVATE", value.Data[0].Secret)
	require.False(t, plan.SupportsFormat("xml"))
	require.False(t, plan.SupportsFormat("tabular"))
	hidden, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name == "Data" {
			copy := *f
			copy.Tag = `json:"-"`
			return &copy, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	denied, err := base.WithRepresentation(hidden)
	require.NoError(t, err)
	require.False(t, denied.SupportsFormat("csv"))
	require.False(t, denied.SupportsFormat("xlsx"))
}

func TestContractExportRowCarriers(t *testing.T) {
	for _, carrier := range []any{
		[]contractFormatRow{{ID: 7, Secret: "PRIVATE", Child: &contractFormatChild{Visible: "public", Secret: "PRIVATE"}}},
		[1]contractFormatRow{{ID: 7, Secret: "PRIVATE", Child: &contractFormatChild{Visible: "public", Secret: "PRIVATE"}}},
		contractFormatRow{ID: 7, Secret: "PRIVATE", Child: &contractFormatChild{Visible: "public", Secret: "PRIVATE"}},
	} {
		t.Run(reflect.TypeOf(carrier).String(), func(t *testing.T) {
			original := reflect.TypeOf(carrier)
			base, err := (Compiler{}).Compile(CompileInput{Type: original, Component: &spec.Component{RootView: &spec.View{Cardinality: spec.CardinalityOne}}})
			require.NoError(t, err)
			walk, err := xshape.CompileRewrite(original)
			require.NoError(t, err)
			metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
				if f.Name == "Secret" {
					copy := *f
					copy.Tag = `json:"-"`
					return &copy, nil
				}
				return nil, nil
			})
			require.NoError(t, err)
			for _, reflected := range []bool{false, true} {
				var plan *Plan
				if reflected {
					typ, err := metadata.Reflected()
					require.NoError(t, err)
					plan, err = base.WithType(typ)
					require.NoError(t, err)
				} else {
					plan, err = base.WithRepresentation(metadata)
					require.NoError(t, err)
				}
				for _, format := range []string{"csv", "xlsx"} {
					text := contractExportText(t, plan, context.Background(), format, carrier)
					require.Contains(t, text, "public")
					require.NotContains(t, text, "PRIVATE")
					require.NotContains(t, strings.ToLower(text), "secret")
				}
			}
		})
	}
}

func TestContractExportNilHolders(t *testing.T) {
	original := reflect.TypeFor[contractFormatOutput]()
	base, err := (Compiler{}).Compile(CompileInput{Type: original, DataField: "Data"})
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(original)
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name == "Secret" {
			copy := *f
			copy.Tag = `json:"-"`
			return &copy, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	plan, err := base.WithRepresentation(metadata)
	require.NoError(t, err)
	for _, value := range []*contractFormatOutput{nil, {}, {Data: []*contractFormatRow{nil, {ID: 7, Secret: "PRIVATE"}}}} {
		for _, format := range []string{"csv", "xlsx"} {
			encoded, err := plan.Encode(context.Background(), format, value)
			require.NoError(t, err)
			require.NotContains(t, string(encoded.Data), "PRIVATE")
		}
	}
}

func TestSpreadsheetDirectArrayExclusions(t *testing.T) {
	value := [1]contractFormatRow{{ID: 7, Secret: "PRIVATE", Child: &contractFormatChild{Visible: "public", Secret: "PRIVATE"}}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Secret", "Child.Secret"}}}}})
	require.NoError(t, err)
	text := contractExportText(t, plan, context.Background(), "xlsx", value)
	require.Contains(t, text, "public")
	require.NotContains(t, text, "PRIVATE")
	require.NotContains(t, strings.ToLower(text), "secret")
}

func TestReflectedContractMapExportFailsClosed(t *testing.T) {
	type row struct {
		Items map[string]contractFormatChild
	}
	typ := reflect.TypeFor[[]row]()
	base, err := (Compiler{}).Compile(CompileInput{Type: typ})
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	effective, err := walk.Rewrite(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name == "Secret" {
			copy := *f
			copy.Tag = `json:"-"`
			return &copy, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	plan, err := base.WithType(effective)
	require.NoError(t, err)
	for _, format := range []string{"csv", "xls", "xlsx"} {
		require.False(t, plan.SupportsFormat(format))
		_, err := plan.Encode(context.Background(), format, []row{{Items: map[string]contractFormatChild{"one": {Secret: "PRIVATE"}}}})
		require.Error(t, err)
		_, err = plan.Wire(format)
		require.Error(t, err)
	}
}

func BenchmarkContractExportFullPath(b *testing.B) {
	original := reflect.TypeFor[contractFormatOutput]()
	base, err := (Compiler{}).Compile(CompileInput{Type: original, DataField: "Data"})
	require.NoError(b, err)
	walk, err := xshape.CompileRewrite(original)
	require.NoError(b, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name == "Secret" {
			copy := *f
			copy.Tag = `json:"-"`
			return &copy, nil
		}
		return nil, nil
	})
	require.NoError(b, err)
	plan, err := base.WithRepresentation(metadata)
	require.NoError(b, err)
	value := &contractFormatOutput{}
	for i := 0; i < 100; i++ {
		value.Data = append(value.Data, &contractFormatRow{ID: i, Secret: "private", Child: &contractFormatChild{Visible: "public", Secret: "private"}})
	}
	for _, format := range []string{"csv", "xlsx"} {
		for _, variant := range []struct {
			name string
			plan *Plan
		}{{"base", base}, {"contract", plan}} {
			b.Run(format+"/"+variant.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_, err := variant.plan.Encode(context.Background(), format, value)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	b.Run("compileContract", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := base.WithRepresentation(metadata)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
