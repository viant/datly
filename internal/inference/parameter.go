package inference

import (
	"fmt"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"go/ast"
	"reflect"
	"strconv"
	"strings"
)

type (
	Parameter struct {
		Explicit bool //explicit parameter are added to the main view as dependency
		state.Parameter
		Repeated State
		Group    State
		ModificationSetting
		SQL         string
		Hint        string
		AssumedType bool
		Connector   string
		InOutput    bool
		Of          string
	}

	ModificationSetting struct {
		IsAuxiliary bool
		IndexField  *Field
		PathParam   *Parameter
	}
)

func (p *Parameter) HasSchema() bool {
	if p.Schema == nil {
		return false
	}
	if p.Schema.DataType == "" && p.Schema.Type() == nil {
		return false
	}
	return true
}

func (p *Parameter) LocalVariable() string {
	upperCamel, _ := formatter.UpperCamel.Caser()
	return upperCamel.Format(p.Name, format.CaseLowerCamel)
}

func (p *Parameter) DsqlParameterDeclaration() string {
	builder := strings.Builder{}
	builder.WriteString("#set($_ = $")
	builder.WriteString(p.Name)
	builder.WriteString("<")
	switch p.In.Kind {
	case state.KindParam:
		builder.WriteString("?")
	default:
		if p.Schema.Cardinality == state.Many {
			builder.WriteString("[]")
		}

		builder.WriteString("*")
		builder.WriteString(p.Schema.DataType)
	}
	builder.WriteString(">(")
	builder.WriteString(string(p.In.Kind))
	builder.WriteByte('/')
	builder.WriteString(string(p.In.Name))
	builder.WriteByte(')')

	if p.SQL != "" {
		builder.WriteString(" /*\n")
		SQL := strings.TrimSpace(p.SQL)
		p.addedValidationModifierIfNeeded(&builder, SQL)
		builder.WriteString(SQL)
		builder.WriteString("\n*/\n")
	}
	builder.WriteByte(')')

	return builder.String()
}

func (p *Parameter) FieldDeclaration() string {
	builder := strings.Builder{}
	if p.SQL != "" {
		p.buildSQLDoc(&builder)
	}

	builder.WriteByte('\t')
	builder.WriteString(p.Name)
	builder.WriteString(" ")
	if p.Schema.Cardinality == state.Many {
		builder.WriteString("[]")
	}
	builder.WriteString("*")

	paramType := p.Schema.Type()
	if p.Schema.DataType != "" {
		builder.WriteString(p.Schema.DataType)
	} else if paramType != nil {
		builder.WriteString(paramType.String())
	}

	tag := fmt.Sprintf(`parameter:"kind=%v,in=%v"`, p.In.Kind, p.In.Name)
	builder.WriteString("`")
	builder.WriteString(tag)
	builder.WriteString("`")
	builder.WriteString(" ")
	return builder.String()
}

func (p *Parameter) buildSQLDoc(builder *strings.Builder) {
	SQL := strings.TrimSpace(p.SQL)
	if SQL == "" {
		return
	}

	builder.WriteString("\n/*\n ")
	p.addedValidationModifierIfNeeded(builder, SQL)
	builder.WriteString(SQL)
	builder.WriteString("\n*/\n")
}

func (p *Parameter) addedValidationModifierIfNeeded(builder *strings.Builder, SQL string) {
	hasValidationModifier := false
	switch SQL[0] {
	case '!', '?':
		hasValidationModifier = true
	}
	if !hasValidationModifier {
		if p.Required != nil && *p.Required {
			builder.WriteString("!")
			if p.ErrorStatusCode > 0 {
				builder.WriteString(strconv.Itoa(p.ErrorStatusCode))
			}
			builder.WriteString(" ")
		} else {
			builder.WriteString("? ")
		}
	}
}

func (p *Parameter) localVariableDefinition() (string, string) {
	upperCamel, _ := formatter.UpperCamel.Caser()
	fieldName := upperCamel.Format(p.Name, format.CaseLowerCamel)
	return fieldName, fmt.Sprintf("%v := state.%v", fieldName, p.Name)
}

func (p *Parameter) IndexVariable() string {
	return p.Name + "By" + p.PathParam.IndexField.Name
}
func buildParameter(field *ast.Field, types *xreflect.Types) (*Parameter, error) {
	SQL := extractSQL(field)
	if field.Tag == nil {
		return nil, nil
	}
	structTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	parameterTag := structTag.Get(state.TagName)
	if parameterTag == "" {
		return nil, nil
	}
	tag, err := state.ParseTag(parameterTag, nil)
	if err != nil {
		return nil, err
	}
	param := &Parameter{
		SQL: SQL,
	}
	//	updateSQLTag(field, SQL)
	param.Name = field.Names[0].Name
	param.In = &state.Location{Name: tag.In, Kind: state.Kind(tag.Kind)}

	cardinality := state.One
	if sliceExpr, ok := field.Type.(*ast.ArrayType); ok {
		field.Type = sliceExpr.Elt
		cardinality = state.Many
	}

	if ptr, ok := field.Type.(*ast.StarExpr); ok {
		field.Type = ptr.X
	}

	fieldType, err := xreflect.Node{Node: field.Type}.Stringify()
	if err != nil {
		return nil, fmt.Errorf("failed to create param: %v due to %w", param.Name, err)
	}
	if strings.Contains(fieldType, "struct{") {
		typeName := ""
		if field.Tag != nil {
			if typeName, _ = reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Lookup("typeName"); typeName == "" {
				typeName = field.Names[0].Name
			}
		}
		rType, err := types.Lookup(typeName, xreflect.WithTypeDefinition(fieldType))
		if err != nil {
			return nil, fmt.Errorf("failed to create param: %v due reflect.Type %w", param.Name, err)
		}
		param.Schema = state.NewSchema(rType)
	} else {
		param.Schema = &state.Schema{DataType: fieldType}
	}

	param.Schema.Cardinality = cardinality
	return param, nil
}

// ParentAlias returns join parent selector
func ParentAlias(join *query.Join) string {
	result := ""
	sqlparser.Traverse(join.On, func(n node.Node) bool {
		switch actual := n.(type) {
		case *qexpr.Binary:
			if xSel, ok := actual.X.(*qexpr.Selector); ok {
				if xSel.Name != join.Alias {
					result = xSel.Name
				}
			}
			if ySel, ok := actual.Y.(*qexpr.Selector); ok {
				if ySel.Name != join.Alias {
					result = ySel.Name
				}
			}
			return true
		}
		return true
	})
	return result
}

func ExtractRelationColumns(join *query.Join) (string, string) {
	relColumn := ""
	refColumn := ""
	sqlparser.Traverse(join.On, func(n node.Node) bool {
		switch actual := n.(type) {
		case *qexpr.Binary:
			if xSel, ok := actual.X.(*qexpr.Selector); ok {
				if xSel.Name == join.Alias {
					refColumn = sqlparser.Stringify(xSel.X)
				} else if relColumn == "" {
					relColumn = sqlparser.Stringify(xSel.X)
				}
			}
			if ySel, ok := actual.Y.(*qexpr.Selector); ok {
				if ySel.Name == join.Alias {
					refColumn = sqlparser.Stringify(ySel.X)
				} else if relColumn == "" {
					relColumn = sqlparser.Stringify(ySel.X)
				}
			}
			return true
		}
		return true
	})
	return relColumn, refColumn
}

func (d *Parameter) EnsureCodec() {
	//if d.Parameter.Codec == nil {
	//	d.Parameter.Codec = &view.Codec{}
	//}
	if d.Parameter.Output == nil {
		d.Parameter.Output = &state.Codec{}
	}
}

func (d *Parameter) EnsureLocation() {
	if d.Parameter.In == nil {
		d.Parameter.In = &state.Location{}
	}
}

func (p *Parameter) HasDataType() bool {
	if p.DataType != "" {
		return true
	}
	if p.Schema == nil {
		return false
	}
	return p.Schema.DataType != ""
}

func (p *Parameter) IsUsedBy(text string) bool {
	parameter := p.Name
	text = strings.ReplaceAll(text, "Unsafe.", "")
	if index := strings.Index(text, "${"+parameter); index != -1 {
		match := text[index+2:]
		if index := strings.Index(match, "}"); index != -1 {
			match = match[:index]
		}
		if p.Name == match {
			return true
		}
	}
	for i := 0; i < len(text); i++ {
		index := strings.Index(text, "$"+parameter)
		if index == -1 {
			break
		}
		text = text[index+1:]
		if len(parameter) == len(text) {
			return true
		}
		terminator := text[len(parameter)]
		if terminator >= 65 && terminator <= 132 {
			continue
		}
		switch terminator {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			continue
		}
		return true
	}
	return false
}

func (d *Parameter) EnsureSchema() {
	if d.Parameter.Schema != nil {
		return
	}
	d.Parameter.Schema = &state.Schema{}
}

func (p *Parameter) MergeFrom(info *Parameter) {
	if p.Output == nil {
		p.Output = info.Output
	}
	if info.DataType != "" {
		p.EnsureSchema()
		p.Schema.DataType = info.DataType
	}
	if info.ErrorStatusCode != 0 {
		p.ErrorStatusCode = info.ErrorStatusCode
	}
}

func (s *Parameter) adjustMetaViewIfNeeded() {
	if !strings.HasPrefix(s.Name, "View.") {
		return
	}
	if strings.HasSuffix(s.Name, ".SQL") {
		s.Schema = state.NewSchema(reflect.TypeOf(""))
		s.Schema.DataType = "string"
	}
	if strings.HasSuffix(s.Name, ".Limit") {
		s.Schema = state.NewSchema(reflect.TypeOf(0))
		s.Schema.DataType = "int"
	}
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

func NewConstParameter(paramName string, paramValue interface{}) *Parameter {
	rType := reflect.TypeOf(paramValue)
	param := &Parameter{
		Parameter: state.Parameter{
			Name:   paramName,
			Const:  paramValue,
			In:     state.NewConstLocation(paramName),
			Schema: state.NewSchema(reflect.TypeOf(paramValue)),
		},
	}
	param.Schema.DataType = rType.Name()
	return param
}

func NewPathParameter(name string) *Parameter {
	return &Parameter{
		Parameter: state.Parameter{
			Name: name,
			In:   state.NewPathLocation(name),
		},
	}
}
