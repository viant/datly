package parser

import (
	"fmt"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"github.com/viant/xreflect"
	"reflect"

	"strconv"
	"strings"
)

type (
	//Declarations defines state (parameters) declaration
	Declarations struct {
		SQL         string
		State       inference.State
		OutputState inference.State
		AsyncState  inference.State
		Transforms  []*marshal.Transform
		lookup      func(dataType string, opts ...xreflect.Option) (*state.Schema, error)
	}
)

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
	if declaration.Transformer != "" || declaration.TransformKind != "" {
		d.Transforms = append(d.Transforms, declaration.Transform())
		return nil
	}
	declaration.ExpandShorthands()

	if declaration.InOutput {
		d.OutputState.Append(&declaration.Parameter)
		return nil
	} else {
		name := declaration.Parameter.Name
		if state.IsReservedAsyncState(name) {
			d.AsyncState.Append(&declaration.Parameter)
		}
	}

	d.State.Append(&declaration.Parameter)
	if authParameter := declaration.AuthParameter(); authParameter != nil {
		if !d.State.Append(authParameter) {
			return fmt.Errorf("parameter %v redeclared", authParameter.Name)
		}
	}
	return nil
}

// IsStructQL returns true if struct QL
func IsStructQL(SQL string) bool {
	query, _ := sqlparser.ParseQuery(SQL)
	if query == nil || query.From.X == nil {
		return false
	}
	from := sqlparser.Stringify(query.From.X)
	return strings.Contains(from, "/")
}

func (d *Declarations) parseExpression(cursor *parsly.Cursor, selector *expr.Select) (*Declaration, error) {
	name := strings.Trim(shared.FirstNotEmpty(selector.FullName, selector.ID), "${}")
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
			declaration.InOutput = declaration.Kind == string(state.KindOutput)
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
			if err := inference.TryUnmarshalHint(hint, hintDeclaration); err != nil {
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
		declaration.Cardinality = state.Many
		dataType = dataType[2:]
	} else if strings.Contains(dataType, "map[") {
		declaration.Cardinality = state.Many
	} else {
		if !strings.HasPrefix(dataType, "*") {
			declaration.Required = &[]bool{true}[0]
		}
		declaration.Cardinality = state.One
	}

	typeName := ""
	if index := strings.Index(dataType, "$"); index != -1 {
		typeName = dataType[index:]
		dataType = strings.Replace(dataType, typeName, "interface{}", 1)
	}

	if dataType != "" {
		if schema, _ := d.lookup(dataType); schema != nil {
			schema.Cardinality = declaration.Cardinality
			if rType := schema.Type(); rType != nil && schema.Cardinality == state.Many {
				if rType.Kind() != reflect.Slice && rType.Kind() != reflect.Map {
					schema.SetType(reflect.SliceOf(rType))
				}
			}
			declaration.Schema = schema
		}
	}
	declaration.EnsureSchema()
	declaration.Schema.DataType = dataType
	declaration.Schema.Name = typeName
	if len(types) > 1 {
		declaration.OutputType = types[1]
	}
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
		args := extractArgs(content)
		switch text {
		case "WithTag":
			if len(args) != 1 {
				return fmt.Errorf("expected WithTag to have one args, but got %v", len(args))
			}

			declaration.Tag = args[0]
		case "WithCodec":
			if len(args) < 1 {
				return fmt.Errorf("expected WithCodec to have at least one arg, but got %v", len(args))
			}

			declaration.Codec = args[0]
			declaration.CodecArgs = args[1:]
		case "WithHandler":
			handler := &state.Handler{}
			declaration.Handler = handler

			if len(args) < 1 {
				return fmt.Errorf("expected WithCodec to have at least one arg, but got %v", len(args))
			}
			declaration.Location = &declaration.Name
			handler.Name = args[0]
			handler.Args = args[1:]

		case "WithStatusCode":
			if len(args) != 1 {
				return fmt.Errorf("expected WithStatusCode to have one arg, but got %v", len(args))
			}
			statusCode, err := strconv.Atoi(args[0])
			if err != nil {
				return err
			}
			declaration.StatusCode = &statusCode
		case "Optional":
			if len(args) != 0 {
				return fmt.Errorf("expected Optional to have zero args, but got %v", len(args))
			}
			required := false
			declaration.Required = &required
		case "Cardinality":
			value := strings.Trim(args[0], `"'`)
			switch strings.ToLower(value) {
			case "one":
				declaration.Cardinality = state.One
			case "many":
				declaration.Cardinality = state.Many
			default:
				return fmt.Errorf("invalid cardinality: %v", args[0])
			}
		case "Required":
			if len(args) != 0 {
				return fmt.Errorf("expected Required to have zero args, but got %v", len(args))
			}
			required := true
			declaration.Required = &required
		case "WithPredicate":
			if err := s.appendPredicate(declaration, args, false); err != nil {
				return err
			}
		case "EnsurePredicate":
			if err := s.appendPredicate(declaration, args, true); err != nil {
				return err
			}
		case "Output":
			declaration.InOutput = true
		case "When":
			declaration.When = strings.Trim(content, "'\"")
		case "Of":
			declaration.Of = strings.Trim(content, "'\"")
			declaration.Name = "." + declaration.Name
		case "Value":
			if err := s.setValue(declaration, content); err != nil {
				return err
			}
		case "QuerySelector":
			declaration.Explicit = false
		case "Async":
			declaration.IsAsync = true
		}
		cursor.MatchOne(whitespaceMatcher)
	}
	return nil
}

func (s *Declarations) setValue(declaration *Declaration, content string) error {
	value := strings.Trim(content, "'\"")
	outputType := declaration.OutputType
	if outputType == "" && declaration.Schema != nil {
		outputType = declaration.Schema.DataType
	}
	var err error
	switch outputType {
	case "bool":
		if declaration.Value, err = strconv.ParseBool(value); err != nil {
			return fmt.Errorf("invalid parameter: %s bool default value: %s %w", declaration.Name, value, err)
		}
	case "int":
		if declaration.Value, err = strconv.Atoi(value); err != nil {
			return fmt.Errorf("invalid parameter: %s int default value: %s %w", declaration.Name, value, err)
		}
	case "float64":
		if declaration.Value, err = strconv.ParseFloat(value, 64); err != nil {
			return fmt.Errorf("invalid parameter: %s float default value: %s %w", declaration.Name, value, err)
		}
	}
	declaration.Value = value
	return nil
}

func (s *Declarations) appendPredicate(declaration *Declaration, args []string, ensure bool) error {
	if len(args) < 2 {
		return fmt.Errorf("expected WithPredicate to have at least 2 args, but got %v", len(args))
	}

	ctx, err := strconv.Atoi(args[0])
	if err != nil {
		return err
	}
	declaration.Predicates = append(declaration.Predicates, &extension.PredicateConfig{
		Name:   args[1],
		Group:  ctx,
		Args:   args[2:],
		Ensure: ensure,
	})
	return nil
}

func extractArgs(content string) []string {
	result := make([]string, 0)
	cursor := parsly.NewCursor("", []byte(content), 0)
	for {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, singleQuotedMatcher, quotedMatcher, comaTerminatedMatcher)
		switch matched.Code {
		case singleQuotedToken, doubleQuotedToken:
			text := matched.Text(cursor)
			arg := extractArg(text)
			result = append(result, arg)
			cursor.MatchOne(comaTerminatedMatcher)
		case comaTerminatedToken:
			text := matched.Text(cursor)
			text = text[:len(text)-1]
			arg := extractArg(text)
			result = append(result, arg)
		default:
			if cursor.Pos < len(cursor.Input) {
				arg := strings.Trim(strings.TrimSpace(string(cursor.Input[cursor.Pos:])), `"'`)
				if len(arg) != 0 {
					result = append(result, arg)
				}
			}
			return result
		}
	}
}

func extractArg(cursorContent string) string {
	text := cursorContent
	text = strings.TrimSpace(text)
	if strings.HasSuffix(text, ",") {
		text = text[:len(text)-1]
	}

	theQuote := text[0]
	switch theQuote {
	case '\'', '"':
		text = strings.Trim(text, string(theQuote))
	}

	return strings.TrimSpace(text)
}

func NewDeclarations(SQL string, lookup func(dataType string, opts ...xreflect.Option) (*state.Schema, error)) (*Declarations, error) {
	result := &Declarations{
		SQL:    SQL,
		State:  nil,
		lookup: lookup,
	}
	return result, result.Init()
}
