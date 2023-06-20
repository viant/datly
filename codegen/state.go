package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
	"go/ast"
	"go/parser"
	"path"
	"reflect"
	"strings"
)

type State []*Parameter

func (s *State) Append(param ...*Parameter) {
	*s = append(*s, param...)
}

//go:embed tmpl/state.gox
var stateGoTemplate string

func (s State) GenerateDSQLDeclration() string {
	var result []string
	for _, param := range s {
		result = append(result, param.DsqlParameterDeclaration())
	}
	return strings.Join(result, "\n\t")
}

func (s State) GenerateGoCode(pkg string) string {
	if pkg == "" {
		pkg = "main"
	}
	if len(s) == 0 {
		return ""
	}
	var output = strings.Replace(stateGoTemplate, "$Package", pkg, 1)

	var fields = []string{}
	for _, input := range s {
		fields = append(fields, input.FieldDeclaration())
	}
	output = strings.Replace(output, "$Fields", strings.Join(fields, "\n\n"), 1)
	return output
}

func NewState(modulePath, dataType string, lookup xreflect.TypeLookupFn) (State, error) {
	baseDir := modulePath
	if pair := strings.Split(dataType, "."); len(pair) > 1 {
		baseDir = path.Join(baseDir, pair[0])
		dataType = pair[1]
	}

	var state = State{}
	dirTypes, err := xreflect.ParseTypes(baseDir,
		xreflect.WithParserMode(parser.ParseComments),
		xreflect.WithTypeLookupFn(lookup),
		xreflect.WithOnField(func(typeName string, field *ast.Field) error {
			fmt.Printf("checking %v\n", typeName)
			if typeName != dataType {
				return nil
			}
			SQL := ""
			if field.Doc != nil { //TODO add
				comments := xreflect.CommentGroup(*field.Doc).Stringify()
				comments = strings.Trim(comments, "\"/**/")
				comments = strings.ReplaceAll(comments, "\t", "  ")
				comments = strings.ReplaceAll(comments, "\n", " ")
				SQL = strings.TrimSpace(comments)
			}
			if field.Tag == nil {
				return nil
			}
			structTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
			datlyTag := structTag.Get(view.DatlyTag)
			if datlyTag == "" {
				return nil
			}
			tag := view.ParseTag(datlyTag)
			param := &Parameter{SQL: SQL}
			param.Name = field.Names[0].Name
			param.In = &view.Location{Name: tag.In, Kind: view.Kind(tag.Kind)}
			fieldTypeName, err := xreflect.Node{Node: field.Type}.Stringify()
			if err != nil {
				return fmt.Errorf("failed to create param: %v due to %w", param.Name, err)
			}
			if strings.Contains(fieldTypeName, "struct{") {
				rType, err := types.ParseType(fieldTypeName, lookup)
				if err != nil {
					return fmt.Errorf("failed to create param: %v due reflect.Type %w", param.Name, err)
				}
				param.Schema = view.NewSchema(rType)
			} else {
				param.Schema = &view.Schema{DataType: fieldTypeName}
			}
			state.Append(param)
			return nil
		}))

	if err != nil {
		return nil, err
	}

	_, _ = dirTypes.Type(dataType)

	for _, param := range state {
		if param.Schema.Type() != nil {
			continue
		}
		paramDataType := param.Schema.DataType
		paramType, err := xreflect.ParseWithLookup(paramDataType, false, func(packagePath, packageIdentifier, typeName string) (reflect.Type, error) {
			return dirTypes.Type(typeName)
		})

		if err != nil {
			return nil, fmt.Errorf("invalid parameter '%v' schema: '%v'  %w", param.Name, param.Schema.DataType, err)

		}
		param.Schema = view.NewSchema(paramType)
		param.Schema.DataType = paramDataType

	}
	return state, nil
}
