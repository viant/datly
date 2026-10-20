package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestCompileBufferedStatementNullableArguments(t *testing.T) {
	scalars := []string{"int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "bool", "string", "time.Time"}
	for _, method := range []string{"Execute", "ExecuteWithResult"} {
		for _, scalar := range scalars {
			for _, prefix := range []string{"", "*"} {
				typ := prefix + scalar
				t.Run(method+"/"+typ, func(t *testing.T) {
					authority := statementAuthority()
					field := plan.StatementSelector{Path: plan.FieldPath{"Input", "Value"}, Type: spec.TypeRef{Name: typ}, Addressable: true}
					authority["Input.Value"] = field
					args := "$Input.Value"
					if method == "ExecuteWithResult" {
						args = "$Input.ID, " + args
					}
					source := `$dml.` + method + `("INSERT INTO records(value) VALUES (?)", ` + args + `)`
					result, err := (&Compiler{}).CompileStatement(source, authority)
					if err != nil {
						t.Fatal(err)
					}
					if len(result.Arguments) != 1 || !reflect.DeepEqual(result.Arguments[0], field) {
						t.Fatalf("nullable selector/type changed: %+v", result.Arguments)
					}
				})
			}
		}
	}
}
func TestCompileBufferedStatementRejectsNonScalarPointers(t *testing.T) {
	for _, method := range []string{"Execute", "ExecuteWithResult"} {
		for _, typ := range []string{"**int", "**time.Time", "*Record", "Record", "*[]int", "[]int", "*map[string]int", "map[string]int", "*func()", "func()", "*interface{}", "interface{}", "*any", "any", "*domain.Count", "domain.Count", "*Count", "Count", "*uintptr", "*complex128", "***string"} {
			t.Run(method+"/"+typ, func(t *testing.T) {
				authority := statementAuthority()
				authority["Input.Value"] = plan.StatementSelector{Path: plan.FieldPath{"Input", "Value"}, Type: spec.TypeRef{Name: typ}, Addressable: true}
				args := "$Input.Value"
				if method == "ExecuteWithResult" {
					args = "$Input.ID, " + args
				}
				_, err := (&Compiler{}).CompileStatement(`$dml.`+method+`("INSERT INTO records(value) VALUES (?)", `+args+`)`, authority)
				if err == nil || !strings.Contains(err.Error(), "typed scalar field") {
					t.Fatalf("unsupported %s argument accepted: %v", typ, err)
				}
			})
		}
	}
}
func TestCompileBufferedStatementPointerResultRemainsRejected(t *testing.T) {
	for _, typ := range []string{"*int", "*int64", "**int64", "uint64"} {
		t.Run(typ, func(t *testing.T) {
			authority := statementAuthority()
			field := authority["Input.ID"]
			field.Type.Name = typ
			authority["Input.ID"] = field
			_, err := (&Compiler{}).CompileStatement(`$dml.ExecuteWithResult("INSERT INTO records(value) VALUES (?)", $Input.ID, $Input.Name)`, authority)
			if err == nil || !strings.Contains(err.Error(), "addressable signed integer field") {
				t.Fatalf("pointer/unsigned result destination accepted: %v", err)
			}
		})
	}
}
