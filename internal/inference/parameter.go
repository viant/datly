package inference

import (
	"embed"
	_ "embed"
	"fmt"
	"go/ast"
	"path"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
)

type (
	Parameter struct {
		Explicit bool //explicit parameter are added to the main view as dependency
		state.Parameter
		Repeated State
		Object   State
		ModificationSetting
		SQL         string
		Hint        string
		AssumedType bool
		Connector   string
		Cache       string
		Limit       *int
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

func (p *Parameter) DsqlParameterDeclaration() string {
	builder := strings.Builder{}
	p.veltyDeclaration(&builder)
	builder.WriteByte(')')
	return builder.String()
}

func (p *Parameter) DsqlOutputParameterDeclaration() string {
	builder := strings.Builder{}
	p.veltyDeclaration(&builder)
	builder.WriteString(".Output()")
	builder.WriteByte(')')
	return builder.String()
}

func (p *Parameter) veltyDeclaration(builder *strings.Builder) {
	builder.WriteString("#set($_ = $")
	builder.WriteString(p.Name)
	builder.WriteString("<")
	switch p.In.Kind {
	case state.KindParam:
		builder.WriteString("?")
	default:
		if p.Schema.Cardinality == state.Many {
			builder.WriteString("[]")

			switch p.In.Kind {
			case "query", "form", "header":
			default:
				if !p.IsRequired() {
					builder.WriteString("*")
				}
			}

		} else if !p.IsRequired() {
			builder.WriteString("*")
		}
		builder.WriteString(p.Schema.DataType)
	}
	builder.WriteString(">(")
	builder.WriteString(string(p.In.Kind))
	builder.WriteByte('/')
	builder.WriteString(string(p.In.Name))
	builder.WriteByte(')')
	if p.ErrorStatusCode != 0 {
		builder.WriteString(".WithStatusCode(" + strconv.Itoa(p.ErrorStatusCode) + ")")
	}
	if p.URI != "" {
		builder.WriteString(".Scope('" + p.Scope + "')")
	}
	if p.Scope != "" {
		builder.WriteString(".Scope('" + p.Scope + "')")
	}
	if p.Tag != "" {
		builder.WriteString(".WithTag('" + p.Tag + "')")
	}
	if p.Cache != "" {
		builder.WriteString(".WithCache('" + p.Cache + "')")
	}

	if p.Limit != nil {
		builder.WriteString(".WithLimit('" + strconv.Itoa(*p.Limit) + "')")
	}

	if p.Required != nil {
		if !*p.Required {
			builder.WriteString(".Optional()")
		} else {
			builder.WriteString(".Required()")
		}
	}

	if p.Cacheable != nil {
		builder.WriteString(".Cacheable('" + strconv.FormatBool(*p.Cacheable) + "')")
	}
	if p.Connector != "" {
		builder.WriteString(".WithConnector('" + p.Connector + "')")
	}
	if p.Value != "" {
		switch actual := p.Value.(type) {
		case string:
			builder.WriteString(".Value('" + actual + "')")
		case []string:

		}
	}
	if p.Output != nil {
		builder.WriteString(".WithCodec('" + p.Output.Name + "'")
		for i, arg := range p.Output.Args {
			if i > 0 {
				builder.WriteString(",")
			}
			builder.WriteString("'" + arg + "'")
		}
		builder.WriteString(")")
	}

	for _, predicate := range p.Predicates {
		builder.WriteString(".WithPredicate(" + strconv.Itoa(predicate.Group) + ",'" + predicate.Name + "'")
		for _, arg := range predicate.Args {
			builder.WriteString(", '" + arg + "'")
		}
		builder.WriteString(")")
	}

	if p.SQL != "" {
		builder.WriteString(" /*\n")
		SQL := strings.TrimSpace(p.SQL)
		p.addedValidationModifierIfNeeded(builder, SQL)
		builder.WriteString(SQL)
		builder.WriteString("\n*/\n")
	}

}

func (p *Parameter) FieldDeclaration(embedRoot string, embed map[string]string, def *view.TypeDefinition) string {
	builder := strings.Builder{}
	//if p.SQL != "" {
	//	p.buildSQLDoc(&builder)
	//}

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

	aTag := tags.Tag{}
	aTag.Parameter = &tags.Parameter{
		Kind: string(p.In.Kind),
		In:   string(p.In.Name),
	}

	URI := text.DetectCaseFormat(p.Name).Format(p.Name, text.CaseFormatLowerUnderscore)
	URI = strings.ReplaceAll(URI, ".", "")
	key := path.Join(embedRoot, URI) + ".sql"

	if p.SQL != "" {
		embed[key] = strings.TrimSpace(p.SQL)
		switch p.In.Kind {
		case state.KindParam:
			aTag.Codec = &tags.Codec{Name: "structql"}
			aTag.Codec.URI = key
			dataType := def.TypeName()
			if def.Cardinality == state.Many {
				dataType = "[]" + dataType
			}
			aTag.Parameter.DataType = dataType
		default:
			aTag.SQL.URI = key
			aTag.View = &tags.View{}
			aTag.Parameter.In = p.Name ////p.Name[3:]
			aTag.View.Name = p.Name    //p.Name[3:]
			//add parameter extraction from SQL
		}
	}

	builder.WriteString("`")
	tag := aTag.UpdateTag(reflect.StructTag(p.Tag))
	builder.WriteString(string(tag))
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

func (p *Parameter) LocalVariable() string {
	return text.CaseFormatUpperCamel.Format(p.Name, text.CaseFormatLowerCamel)
}

func (p *Parameter) localVariableDefinition() (string, string) {
	fieldName := p.LocalVariable()
	return fieldName, fmt.Sprintf("%v := input.%v", fieldName, p.Name)
}

func (p *Parameter) IndexVariable() string {
	return p.Name + "By" + p.PathParam.IndexField.Name
}

func (p *Parameter) SyncObject() {
	parameter := &p.Parameter
	if len(parameter.Object) > 0 && len(p.Object) == 0 {
		for i, anObject := range parameter.Object {
			iRepeated := &Parameter{Parameter: *anObject}
			p.Object = append(p.Object, iRepeated)
			parameter.Object[i] = &iRepeated.Parameter
		}
	}
}

// TODO unify with state.BuildParameter (by converting field *ast.Field to reflect.StructField)
func buildParameter(field *xunsafe.Field, aTag *tags.Tag, types *xreflect.Types, embedFS *embed.FS, imps xreflect.GoImports, pkg string) (*Parameter, error) {
	//SQL := extractSQL(field)
	if field.Tag == "" {
		return nil, nil
	}
	//TODO convert ast.field to struct field and move that logic to state.BuildParameter
	//currenty there are two places to mange filed tag  to parameter conversion
	structTag := field.Tag
	aTag, err := tags.Parse(structTag, embedFS, tags.ParameterTag, tags.SQLTag, tags.PredicateTag, tags.CodecTag, tags.HandlerTag, tags.ViewTag, format.TagName)
	if err != nil || aTag.Parameter == nil {
		return nil, err
	}
	pTag := aTag.Parameter

	param := &Parameter{}
	if aTag.Codec != nil {
		param.SQL = aTag.Codec.Body
	}
	if aTag.SQL.SQL != "" {
		param.SQL = aTag.SQL.SQL
	}

	if aTag.View != nil {
		param.Connector = aTag.View.Connector
		if aTag.View.Cache != "" {
			param.Cache = aTag.View.Cache
		}
		if aTag.View.Limit != nil {
			param.Limit = aTag.View.Limit
		}
	}

	fType := field.Type
	param.Name = field.Name
	if param.Name == "" {
		if fType.Kind() == reflect.Pointer {
			fType = fType.Elem()
		}
		param.Name = fType.Name()
	}
	if pTag.Name != "" {
		param.Name = pTag.Name
	}
	param.When = pTag.When
	param.Scope = pTag.Scope
	param.Cacheable = pTag.Cacheable
	param.With = pTag.With
	param.URI = pTag.URI
	param.Async = pTag.Async
	param.In = &state.Location{Name: pTag.In, Kind: state.Kind(pTag.Kind)}
	param.Required = pTag.Required

	cardinality := state.One
	if field.Type.Kind() == reflect.Slice {
		cardinality = state.Many
	}
	param.Schema = state.NewSchema(fType)
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

func (p *Parameter) EnsureCodec() {
	//if p.Parameter.Codec == nil {
	//	p.Parameter.Codec = &view.Codec{}
	//}
	if p.Parameter.Output == nil {
		p.Parameter.Output = &state.Codec{}
	}
}

func (p *Parameter) EnsureLocation() {
	if p.Parameter.In == nil {
		p.Parameter.In = &state.Location{}
	}
}

func (p *Parameter) HasDataType() bool {
	if p.Schema == nil {
		return false
	}
	return p.Schema.DataType != ""
}

func (p *Parameter) EnsureSchema() {
	if p.Parameter.Schema != nil {
		return
	}
	p.Parameter.Schema = &state.Schema{}
}

func (p *Parameter) MergeFrom(info *Parameter) {
	if p.Output == nil {
		p.Output = info.Output
	}
	if info.ErrorStatusCode != 0 {
		p.ErrorStatusCode = info.ErrorStatusCode
	}
}

func (p *Parameter) adjustMetaViewIfNeeded() {
	if !strings.HasPrefix(p.Name, "View.") {
		return
	}
	if strings.HasSuffix(p.Name, ".SQL") {
		p.Schema = state.NewSchema(reflect.TypeOf(""))
		p.Schema.DataType = "string"
	}
	if strings.HasSuffix(p.Name, ".Limit") {
		p.Schema = state.NewSchema(reflect.TypeOf(0))
		p.Schema.DataType = "int"
	}
}

func (p *Parameter) IndexFieldDeclaration() string {
	name := p.Name
	if strings.HasPrefix(name, "Cur") {
		name = name[3:]
	}
	return p.IndexVariable() + " Indexed" + name + " `json:\"-\"`"
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
			Value:  paramValue,
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
