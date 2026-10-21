package goliteral

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestStructTagLiteralRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, tag string
		raw       bool
	}{
		{"ordinary", `json:"data,omitempty" parameter:"Data,kind=body"`, true},
		{"bigquery", "view:\"rows,table=`project.dataset.table`\"", false},
		{"escaped quotes and backslashes", `view:"rows" sql:"SELECT \"name\", 'C:\\data'"`, true},
		{"literal escape sequences", `json:"data" note:"line\nnext\tcolumn"`, true},
		{"tab", "json:\"data\"\tnote:\"tab\"", true},
		{"newline", "json:\"data\"\nnote:\"line\"", false},
		{"carriage return", "json:\"data\"\rnote:\"line\"", false},
		{"control", "note:\"\x00\x01\x7f\"", false},
		{"unicode", `json:"café" note:"日本語"`, true},
		{"invalid UTF8", "note:\"\xff\"", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			literal := StructTag(tc.tag)
			if strings.HasPrefix(literal, "`") != tc.raw {
				t.Fatalf("literal preference: %s", literal)
			}
			value, err := strconv.Unquote(literal)
			if err != nil || value != tc.tag {
				t.Fatalf("unquote = %q, %v; want %q", value, err, tc.tag)
			}
			file, err := parser.ParseFile(token.NewFileSet(), "tags.go", "package example\ntype Output struct {Data string "+literal+"}\n", 0)
			if err != nil {
				t.Fatal(err)
			}
			var rendered bytes.Buffer
			if err = format.Node(&rendered, token.NewFileSet(), file); err != nil {
				t.Fatal(err)
			}
			reparsed, err := parser.ParseFile(token.NewFileSet(), "tags.go", rendered.Bytes(), 0)
			if err != nil {
				t.Fatal(err)
			}
			field := reparsed.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType).Fields.List[0]
			roundtrip, err := strconv.Unquote(field.Tag.Value)
			if err != nil || roundtrip != tc.tag {
				t.Fatalf("Go AST roundtrip = %q, %v", roundtrip, err)
			}
			typ := reflect.StructOf([]reflect.StructField{{Name: "Data", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(roundtrip)}})
			if string(typ.Field(0).Tag) != tc.tag {
				t.Fatal("reflection tag bytes changed")
			}
			for _, key := range []string{"json", "parameter", "view", "sql", "note"} {
				if typ.Field(0).Tag.Get(key) != reflect.StructTag(tc.tag).Get(key) {
					t.Fatalf("reflection value %s changed", key)
				}
			}
		})
	}
}
