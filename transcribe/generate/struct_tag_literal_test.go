package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestGeneratedStructTagLiteralPreference(t *testing.T) {
	for _, tag := range []string{`json:"value" view:"records"`, "view:\"records,table=`project.dataset.table`\"", `json:"value" description:"escaped \"name\" C:\\tmp"`, "json:\"value\" description:\"actual\rreturn\""} {
		t.Run(tag, func(t *testing.T) {
			want := generatedStructTag(tag)
			fields := []Field{{Name: "Value", Type: "string", Tag: tag}}
			var contract strings.Builder
			appendFields(&contract, fields)
			var holder strings.Builder
			writeComponentHolderField(&holder, "Contract", &Plan{Input: ContractPlan{Type: "Input"}, Output: ContractPlan{Type: "Output"}}, tag, "xdatly")
			typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(tag)}})
			cube, err := cubeTypeExpression(typ, "", func(pkg, alias string) string { return alias })
			if err != nil {
				t.Fatal(err)
			}
			for name, source := range map[string]string{"contract": "struct {" + contract.String() + "}", "holder": "struct {" + holder.String() + "}", "cube": cube} {
				expr, err := parser.ParseExpr(source)
				if err != nil {
					t.Fatalf("%s: %v\n%s", name, err, source)
				}
				literal := expr.(*ast.StructType).Fields.List[0].Tag
				if literal == nil || literal.Kind != token.STRING {
					t.Fatalf("%s: tag missing", name)
				}
				decoded, err := strconv.Unquote(literal.Value)
				if err != nil || decoded != want {
					t.Fatalf("%s: tag=%q err=%v want=%q", name, decoded, err, want)
				}
				if strings.HasPrefix(literal.Value, "`") != strconv.CanBackquote(want) {
					t.Fatalf("%s: wrong literal preference: %s", name, literal.Value)
				}
			}
		})
	}
}
