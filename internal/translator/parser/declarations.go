package parser

import (
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"github.com/viant/xreflect"

	"strconv"
	"strings"
)

type (
	//Declarations defines state (parameters) declaration
	Declarations struct {
		SQL          string
		State        inference.State
		Transforms   []*marshal.Transform
		imports      TypeImports
		typeRegistry *xreflect.Types
	}
)

func (d *Declarations) ensureRegistry() *xreflect.Types {
	if d.typeRegistry != nil {
		return d.typeRegistry
	}
	d.typeRegistry = xreflect.NewTypes(xreflect.WithRegistry(config.Config.Types))
	return d.typeRegistry
}

func (d *Declarations) Init() error {
	SQLBytes := []byte(" " + d.SQL)
	cursor := parsly.NewCursor("", SQLBytes, 0)
	for {
		matched := cursor.MatchOne(setTerminatedMatcher)
		switch matched.Code {
		case setTerminatedToken:
			setStart := cursor.Pos
			cursor.MatchOne(setMatcher) //to move cursor
			matched = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
			if matched.Code != exprGroupToken {
				continue
			}
			content := matched.Text(cursor)
			content = content[1 : len(content)-1]
			contentCursor := parsly.NewCursor("", []byte(content), 0)

			matched = contentCursor.MatchAfterOptional(whitespaceMatcher, parameterDeclarationMatcher)
			if matched.Code != parameterDeclarationToken {
				continue
			}
			matched = contentCursor.MatchOne(whitespaceMatcher)
			selector, err := parser.MatchSelector(contentCursor)
			if err != nil {
				continue
			}
			if err = d.buildDeclaration(selector, contentCursor); err != nil {
				return err
			}
			for i := setStart; i < cursor.Pos; i++ {
				SQLBytes[i] = ' '
			}

		default:
			d.SQL = strings.TrimSpace(string(SQLBytes))
			return nil
		}
	}
}

func (d *Declarations) buildDeclaration(selector *expr.Select, cursor *parsly.Cursor) error {
	declaration, err := d.parseExpression(cursor, selector)
	if declaration == nil || err != nil {
		return err
	}

	if declaration.Transformer != "" {
		d.Transforms = append(d.Transforms, declaration.Transform())
		return nil
	}
	declaration.ExpandShorthands()
	d.State.Append(&declaration.Parameter)
	if authParameter := declaration.AuthParameter(); authParameter != nil {
		if !d.State.Append(authParameter) {
			return fmt.Errorf("parameter %v redeclared", authParameter.Name)
		}
	}
	return nil
}

func IsStructQL(SQL string) bool {
	query, _ := sqlparser.ParseQuery(SQL)
	if query == nil || query.From.X == nil {
		return false
	}
	from := sqlparser.Stringify(query.From.X)
	return strings.Contains(from, "/")
}

func (d *Declarations) parseExpression(cursor *parsly.Cursor, selector *expr.Select) (*Declaration, error) {
	name := strings.Trim(view.FirstNotEmpty(selector.FullName, selector.ID), "${}")
	declaration := &Declaration{}
	declaration.Name = name
	declaration.Explicit = true
	possibilities := []*parsly.Token{typeMatcher, exprGroupMatcher}
	for len(possibilities) > 0 {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, possibilities...)
		switch matched.Code {
		case typeToken: //< >
			typeContent := matched.Text(cursor)
			typeContent = strings.TrimSpace(typeContent[1 : len(typeContent)-1])
			d.tryParseTypeExpression(typeContent, declaration)
			possibilities = []*parsly.Token{exprGroupMatcher}

		case exprGroupToken: //(...)
			inContent := matched.Text(cursor)
			inContent = strings.TrimSpace(inContent[1 : len(inContent)-1])
			segments := strings.Split(inContent, "/")
			declaration.Kind = segments[0]
			location := ""
			if len(segments) > 1 {
				location = strings.Join(segments[1:], ".")
			}
			declaration.Location = &location
			if err := d.parseShorthands(declaration, cursor); err != nil {
				return nil, err
			}
			possibilities = []*parsly.Token{}
		default:
			possibilities = []*parsly.Token{}
		}
	}
	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentMatcher)
	if matched.Code == commentToken { // /* .. */
		aComment := matched.Text(cursor)
		aComment = aComment[2 : len(aComment)-2]
		hint, SQL := SplitHint(aComment)
		declaration.SQL = SQL
		if hint != "" {
			hintDeclaration := &Declaration{}
			if err := TryUnmarshalHint(hint, hintDeclaration); err != nil {
				return nil, fmt.Errorf("invalid declaration %v, unable parse hint: %w", declaration.Name, err)
			}
			merged, err := declaration.Merge(hintDeclaration)
			if err != nil {
				return nil, err
			}
			return merged, nil
		}
	}
	return declaration, nil
}

func (d *Declarations) tryParseTypeExpression(typeContent string, declaration *Declaration) {
	if typeContent == "?" {
		return
	}
	types := strings.Split(typeContent, ",")
	dataType := types[0]

	if strings.HasPrefix(dataType, "[]") {
		declaration.Cardinality = view.Many
		dataType = dataType[2:]
	} else {
		declaration.Cardinality = view.One
	}

	if dataType != "" {
		if schema, _ := d.GetSchema(dataType); schema != nil {
			schema.Cardinality = declaration.Cardinality
			declaration.Schema = schema
		}
	}
	declaration.EnsureSchema()
	declaration.Schema.DataType = dataType

	if len(types) > 1 {
		declaration.OutputType = types[1]

	}
}

func (d *Declarations) GetSchema(dataType string) (*view.Schema, error) {
	if importsSpec := d.imports.Lookup(dataType); importsSpec != nil {
		registry := d.ensureRegistry()
		rType, err := registry.Lookup(dataType, xreflect.WithPackagePath(importsSpec.URL))
		if err != nil {
			return nil, err
		}

		pkg := ""

		if pkgSymbol, err := registry.Symbol("PackageName"); err == nil {
			if text, ok := pkgSymbol.(string); ok {
				pkg = text
			}
		}

		if methods, _ := registry.Methods(dataType); len(methods) > 0 {
			importsSpec.Methods = methods
		}
		schema := view.NewSchema(rType)
		if strings.HasPrefix(dataType, "*") {
			dataType = dataType[1:]
		}
		schema.DataType = dataType
		typeDef := &view.TypeDefinition{Name: dataType, DataType: rType.String()}
		typeDef.Package = pkg
		schema.Package = pkg
		schema.Methods = importsSpec.Methods
		/*
			if importsSpec.ForceGoTypeUse {
				typeDef.DataType = ""
				typeDef.Schema = &view.Schema{DataType: dataType}
			}
		*/
		importsSpec.AppendTypeDefinition(typeDef)
		return schema, nil
	}
	return &view.Schema{DataType: dataType}, nil
}

func (s *Declarations) parseShorthands(declaration *Declaration, cursor *parsly.Cursor) error {
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchOne(dotMatcher)
		if matched.Code != dotToken {
			return nil
		}
		matched = cursor.MatchOne(selectMatcher)
		if matched.Code != selectToken {
			return cursor.NewError(selectMatcher)
		}

		text := matched.Text(cursor)
		matched = cursor.MatchOne(exprGroupMatcher)
		if matched.Code != exprGroupToken {
			return cursor.NewError(exprGroupMatcher)
		}

		content := matched.Text(cursor)
		content = content[1 : len(content)-1]
		switch text {
		case "WithCodec":
			declaration.Codec = strings.Trim(content, "'")
		case "WithStatusCode":
			statusCode, err := strconv.Atoi(content)
			if err != nil {
				return err
			}
			declaration.StatusCode = &statusCode
		case "UtilParam":

		}
		cursor.MatchOne(whitespaceMatcher)
	}
	return nil
}

func NewDeclarations(SQL string, imports TypeImports) (*Declarations, error) {
	result := &Declarations{
		SQL:     SQL,
		State:   nil,
		imports: imports,
	}
	return result, result.Init()
}
