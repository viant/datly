package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/tagly/tags"
)

func TestReplaceStructTagPreservesPosition(t *testing.T) {
	for _, tt := range []struct{ name, raw, value, want string }{
		{"first", `parameter:"old" internal:"true"`, "new", `parameter:"new" internal:"true"`},
		{"middle", `json:"" parameter:"old" internal:"true"`, "new", `json:"" parameter:"new" internal:"true"`},
		{"last", `internal:"true" parameter:"old"`, "new", `internal:"true" parameter:"new"`},
		{"empty", `parameter:"old" json:""`, "", `parameter:"" json:""`},
		{"append", `internal:"true" json:""`, "new", `internal:"true" json:"" parameter:"new"`},
		{"duplicates", `parameter:"old" json:"" parameter:"other"`, "new", `parameter:"new" json:""`},
		{"unchanged bytes", `  parameter:"\x61"   json:"" `, "a", `  parameter:"\x61"   json:"" `},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceStructTag(tt.raw, "parameter", tt.value)
			require.Equal(t, tt.want, got)
			require.Equal(t, got, replaceStructTag(got, "parameter", tt.value))
		})
	}
}

func TestGeneratedStructTagOrdering(t *testing.T) {
	const parameter = "Table,kind=const,in=Table,dataType=string,value=CI_FEATURE_AGGREGATION_CONSTRAINT"
	first := `parameter:"` + parameter + `" internal:"true" json:"" example:"quote\" slash\\"`
	second := `example:"quote\" slash\\" json:"" internal:"true" parameter:"` + parameter + `"`
	want := `example:"quote\" slash\\" internal:"true" json:"" parameter:"` + parameter + `"`
	require.Equal(t, want, generatedStructTag(first))
	require.Equal(t, want, generatedStructTag(second))
	require.Equal(t, want, generatedStructTag(want))
	parse := func(raw string) *dtag.Field {
		field, err := dtag.ParseField(reflect.StructField{Name: "Table", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(raw)})
		require.NoError(t, err)
		return field
	}
	require.Equal(t, parse(first), parse(second))
	require.Equal(t, parse(first), parse(want), "outer-key ordering must not change binding semantics")
	for _, key := range []string{"parameter", "internal", "json", "example"} {
		a, present := reflect.StructTag(want).Lookup(key)
		b, original := reflect.StructTag(first).Lookup(key)
		require.True(t, present)
		require.True(t, original)
		require.Equal(t, b, a)
	}
	// Sorting must neither collapse duplicate keys nor hide malformed suffixes.
	require.Equal(t, `json:"a" json:"b" z:""`, generatedStructTag(`z:"" json:"a" json:"b"`))
	const malformed = `parameter:"ok" invalid`
	require.Equal(t, malformed, generatedStructTag(malformed))
	_, err := tags.Parse(generatedStructTag(malformed))
	require.Error(t, err)

	fields := []Field{{Name: "Table", Type: "string", Tag: first}}
	var b strings.Builder
	appendFields(&b, fields)
	require.Contains(t, b.String(), "`"+want+"`")
	require.Equal(t, first, fields[0].Tag, "emission must not mutate plan metadata")
}

func TestGeneratedTagRewritesPreserveEmptyValues(t *testing.T) {
	const raw = `json:"" custom:"" internal:"true"`
	require.Equal(t, raw, contractFieldTags(raw))
	require.Equal(t, `json:"" custom:""`, withoutStructTags(raw, "internal"))
	column := &spec.Column{Name: "Value", Tag: `example:"" json:"value,omitempty"`}
	got := scalarColumnFieldTag(column, "value", false)
	value, present := reflect.StructTag(got).Lookup("example")
	require.True(t, present)
	require.Empty(t, value)
	require.Equal(t, "value,omitempty", reflect.StructTag(got).Get("json"))
}

func TestBorrowedStructAcceptsGeneratedTagOrdering(t *testing.T) {
	const original = `sqlx:"id,primaryKey=true,required=false" json:"id" custom:""`
	want := []Field{{Name: "Id", Type: "int", Tag: original}}
	for _, tt := range []struct {
		tag   string
		valid bool
	}{
		{original, true},
		{`custom:"" json:"id" sqlx:"id,primaryKey=true,required=false"`, true},
		{`custom:"" json:"id" sqlx:"id,required=false,primaryKey=true"`, false},
		{`custom:"" json:"other" sqlx:"id,primaryKey=true,required=false"`, false},
		{`json:"id" sqlx:"id,primaryKey=true,required=false"`, false},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "row.go", "package rows\ntype Row struct { Id int "+strconv.Quote(tt.tag)+" }", 0)
		require.NoError(t, err)
		decl := file.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec)
		err = validateBorrowedStruct(decl, nil, "example.com/rows", want)
		if tt.valid {
			require.NoError(t, err, tt.tag)
		} else {
			require.Error(t, err, tt.tag)
		}
	}
}
