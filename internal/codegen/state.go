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
	"strconv"
	"strings"
)

type State []*Parameter

func (s *State) Append(param ...*Parameter) {
	*s = append(*s, param...)
}

func (s State) BodyParameter() *Parameter {
	for _, param := range s {
		if param.In.Kind == view.KindRequestBody {
			return param
		}
	}
	return nil
}

func (s State) dsqlParameterDeclaration() string {
	var result []string
	for _, param := range s {
		result = append(result, param.DsqlParameterDeclaration())
	}
	return strings.Join(result, "\n\t")
}

func (s State) ensureSchema(dirTypes *xreflect.DirTypes) error {
	for _, param := range s {
		if param.Schema.Type() != nil {
			continue
		}
		paramDataType := param.Schema.DataType
		paramType, err := xreflect.ParseWithLookup(paramDataType, false, func(packagePath, packageIdentifier, typeName string) (reflect.Type, error) {
			return dirTypes.Type(typeName)
		})
		if err != nil {
			return fmt.Errorf("invalid parameter '%v' schema: '%v'  %w", param.Name, param.Schema.DataType, err)
		}

		oldSchema := param.Schema
		param.Schema = view.NewSchema(paramType)
		param.Schema.DataType = paramDataType

		if oldSchema != nil {
			param.Schema.Cardinality = oldSchema.Cardinality
		}
	}
	return nil
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
			if field.Tag == nil {
				return nil
			}
			datlyTag, _ := reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Lookup(view.DatlyTag)
			if datlyTag == "" {
				return nil
			}
			tag := view.ParseTag(datlyTag)
			if tag.Kind == "" {
				return nil
			}
			param, err := buildParameter(field, lookup)
			if param == nil {
				return err
			}
			state.Append(param)
			return nil
		}))

	if err != nil {
		return nil, err
	}
	if _, err = dirTypes.Type(dataType); err != nil {
		return nil, err
	}
	if err = state.ensureSchema(dirTypes); err != nil {
		return nil, err
	}

	return state, nil
}

func buildParameter(field *ast.Field, lookup xreflect.TypeLookupFn) (*Parameter, error) {
	SQL := extractSQL(field)
	if field.Tag == nil {
		return nil, nil
	}
	structTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	datlyTag := structTag.Get(view.DatlyTag)
	if datlyTag == "" {
		return nil, nil
	}
	tag := view.ParseTag(datlyTag)
	param := &Parameter{SQL: SQL}
	//	updateSQLTag(field, SQL)
	param.Name = field.Names[0].Name
	param.In = &view.Location{Name: tag.In, Kind: view.Kind(tag.Kind)}

	cardinality := view.One
	if sliceExpr, ok := field.Type.(*ast.ArrayType); ok {
		field.Type = sliceExpr.Elt
		cardinality = view.Many
	}

	if ptr, ok := field.Type.(*ast.StarExpr); ok {
		field.Type = ptr.X
	}

	fieldTypeName, err := xreflect.Node{Node: field.Type}.Stringify()
	if err != nil {
		return nil, fmt.Errorf("failed to create param: %v due to %w", param.Name, err)
	}
	if strings.Contains(fieldTypeName, "struct{") {
		rType, err := types.ParseType(fieldTypeName, lookup)
		if err != nil {
			return nil, fmt.Errorf("failed to create param: %v due reflect.Type %w", param.Name, err)
		}
		param.Schema = view.NewSchema(rType)
	} else {
		param.Schema = &view.Schema{DataType: fieldTypeName}
	}

	param.Schema.Cardinality = cardinality
	return param, nil
}

func updateSQLTag(field *ast.Field, SQL string) {
	if SQL == "" {
		return
	}

	SQL = strings.ReplaceAll(SQL, "\n", "   ")
	field.Tag.Value = "`" + strings.Trim(field.Tag.Value, "`") + fmt.Sprintf(` sql:%v`, strconv.Quote(SQL)) + "`"

}

func extractSQL(field *ast.Field) string {
	SQL := ""
	if field.Doc != nil {
		comments := xreflect.CommentGroup(*field.Doc).Stringify()
		comments = strings.Trim(comments, "\"/**/")
		comments = strings.ReplaceAll(comments, "\t", "  ")
		comments = strings.ReplaceAll(comments, "\n", " ")
		SQL = strings.TrimSpace(comments)
	}
	return SQL
}

//go:embed tmpl/state.gox
var stateGoTemplate string

func (t *Template) GenerateState(pkg string) string {
	pkg = t.getPakcage(pkg)
	if len(t.State) == 0 {
		return ""
	}
	var output = strings.Replace(stateGoTemplate, "$Package", pkg, 1)
	var fields = []string{}
	for _, input := range t.State {
		fields = append(fields, input.FieldDeclaration())
	}
	output = strings.Replace(output, "$Fields", strings.Join(fields, "\n\n"), 1)
	return output
}

func (t *Template) getPakcage(pkg string) string {
	if pkg == "" {
		if t.TypeDef != nil {
			pkg = t.TypeDef.Package
		}
	}
	if pkg == "" {
		pkg = "main"
	}
	return pkg
}

func (t *Template) buildState(spec *Spec, state *State, card view.Cardinality) {
	t.Imports.AddType(spec.Type.TypeName())
	if param := t.buildPathParameterIfNeeded(spec); param != nil {
		state.Append(param)
	}
	if spec.Type.Cardinality == view.Many {
		card = view.Many
	}
	state.Append(t.buildDataViewParameter(spec, card))
	for _, rel := range spec.Relations {
		t.buildState(rel.Spec, state, rel.Cardinality)
	}
}

func (t *Template) buildPathParameterIfNeeded(spec *Spec) *Parameter {
	selector := spec.Selector()
	field, SQL := spec.pkStructQL(selector)
	if SQL == "" {
		return nil
	}
	param := &Parameter{}
	parameterNamer := t.ColumnParameterNamer(selector)
	param.Name = parameterNamer(field.Column)
	param.SQL = SQL
	param.In = &view.Location{Kind: view.KindParam, Name: selector[0]}
	var paramType = reflect.StructOf([]reflect.StructField{{Name: "Values", Type: reflect.SliceOf(field.Schema.Type())}})
	param.Schema = view.NewSchema(paramType)
	return param
}

func (t *Template) buildDataViewParameter(spec *Spec, cardinality view.Cardinality) *Parameter {
	param := &Parameter{}
	param.Name = t.ParamName(spec.Type.Name)
	param.Schema = &view.Schema{DataType: spec.Type.Name, Cardinality: cardinality}
	param.In = &view.Location{Kind: view.KindDataView, Name: param.Name}
	param.SQL = spec.viewSQL(t.ColumnParameterNamer(spec.Selector()))
	return param
}
