package parser

import (
	"encoding/json"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"strings"
)

type (
	ParametersDeclarations struct {
		SQL               string
		ParamDeclarations []*ParameterDeclaration
		Transforms        []*marshal.Transform
	}

	ParameterDeclaration struct {
		inference.Parameter
	}

	paramJSONHintConfig struct {
		option.ParameterConfig
		option.TransformOption
	}
)

func NewParameterDeclarations(SQL string) (*ParametersDeclarations, error) {
	result := &ParametersDeclarations{
		SQL:               SQL,
		ParamDeclarations: nil,
	}

	return result, result.Init()
}

func (d *ParametersDeclarations) Init() error {
	SQLBytes := []byte(d.SQL)
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

			if err = d.buildParamHint(selector, contentCursor); err != nil {
				return err
			}

			for i := setStart; i < cursor.Pos; i++ {
				SQLBytes[i] = ' '
			}

		default:
			return nil
		}
	}
}

func (d *ParametersDeclarations) buildParamHint(selector *expr.Select, cursor *parsly.Cursor) error {
	paramHint, err := d.parseParamHint(cursor)
	if paramHint == "" || err != nil {
		return err
	}

	holderName := strings.Trim(view.FirstNotEmpty(selector.FullName, selector.ID), "${}")
	hint, SQL := sanitize.SplitHint(paramHint)

	if pathStartIndex := strings.Index(holderName, "."); pathStartIndex >= 0 {
		aTransform := &option.TransformOption{}
		if err = d.tryUnmarshalHint(hint, aTransform); err != nil {
			return err
		}

		_, paramName := sanitize.GetHolderName(holderName)
		d.Transforms = append(d.Transforms, &marshal.Transform{
			ParamName:   paramName,
			Kind:        aTransform.TransformKind,
			Path:        holderName[pathStartIndex+1:],
			Codec:       aTransform.Codec,
			Source:      strings.TrimSpace(SQL),
			Transformer: aTransform.Transformer,
		})

		return nil
	}

	qlQuery, _ := sanitize.TryParseStructQLHint(paramHint)
	if qlQuery == nil {
		paramConfig := option.ParameterConfig{}
		hint, sqlQuery := sanitize.SplitHint(paramHint)
		if err = d.tryUnmarshalHint(hint, &paramConfig); err != nil {
			return err
		}

		if paramConfig.Kind == string(view.KindParam) && paramConfig.Location != nil {
			qlQuery = &sanitize.StructQLQuery{
				SQL:    sqlQuery,
				Source: *paramConfig.Location,
			}
		}
	}

	d.addParamHint(holderName, paramHint, qlQuery)

	builder.paramsIndex.AddParamHint(holderName)

	return nil
}

func (d *ParametersDeclarations) addParamHint(holderName string, paramHint string, qlQuery *sanitize.StructQLQuery) {
	parameterDeclaration := d.ParamByName(holderName)
	parameterDeclaration.Hint = paramHint
	parameterDeclaration.In = &view.Location{
		Name: holderName,
		Kind: view.KindParam,
	}
}

func (d *ParametersDeclarations) ParamByName(holderName string) *ParameterDeclaration {
	var parameterDeclaration *ParameterDeclaration
	for _, declaration := range d.ParamDeclarations {
		if declaration.Name == holderName {
			parameterDeclaration = declaration
		}
	}

	if parameterDeclaration == nil {
		parameterDeclaration = &ParameterDeclaration{}
		d.ParamDeclarations = append(d.ParamDeclarations, parameterDeclaration)
	}
	return parameterDeclaration
}

func (d *ParametersDeclarations) parseParamHint(cursor *parsly.Cursor) (string, error) {
	aConfig := &paramJSONHintConfig{}
	possibilities := []*parsly.Token{typeMatcher, exprGroupMatcher}
	for len(possibilities) > 0 {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, possibilities...)
		switch matched.Code {
		case typeToken:
			typeContent := matched.Text(cursor)
			typeContent = strings.TrimSpace(typeContent[1 : len(typeContent)-1])
			d.tryUpdateConfigType(typeContent, aConfig)
			possibilities = []*parsly.Token{exprGroupMatcher}

		case exprGroupToken:
			inContent := matched.Text(cursor)
			inContent = strings.TrimSpace(inContent[1 : len(inContent)-1])
			segments := strings.Split(inContent, "/")
			aConfig.Kind = segments[0]

			target := ""
			if len(segments) > 1 {
				target = strings.Join(segments[1:], ".")
			}

			aConfig.Location = &target

			if err := d.readParamConfigs(&aConfig.ParameterConfig, cursor); err != nil {
				return "", err
			}
			possibilities = []*parsly.Token{}
		default:
			possibilities = []*parsly.Token{}
		}
	}

	matched := cursor.MatchAfterOptional(whitespaceMatcher, commentMatcher)
	actualHint := map[string]interface{}{}
	var sql string
	if matched.Code == commentToken {
		aComment := matched.Text(cursor)
		aComment = aComment[2 : len(aComment)-2]

		hint, SQL := sanitize.SplitHint(aComment)
		if hint != "" {
			if err := json.Unmarshal([]byte(hint), &actualHint); err != nil {
				return "", err
			}
		}

		sql = SQL
	}

	configJson, err := mergeJsonStructs(aConfig.TransformOption, aConfig.ParameterConfig, actualHint)
	if err != nil {
		return "", err
	}

	result := string(configJson)
	if sql != "" {
		result += " " + sql
	}

	return result, nil
}

func (d *ParametersDeclarations) tryUpdateConfigType(typeContent string, aConfig *paramJSONHintConfig) {
	if typeContent == "?" {
		return
	}

	types := strings.Split(typeContent, ",")
	dataType := types[0]
	if strings.HasPrefix(dataType, "[]") {
		aConfig.Cardinality = view.Many
		dataType = dataType[2:]
	} else {
		aConfig.Cardinality = view.One
	}

	aConfig.DataType = dataType
	if len(types) > 1 {
		aConfig.CodecType = types[1]
	}
}

func (d *ParametersDeclarations) mergeJsonStructs(args ...interface{}) ([]byte, error) {
	result := map[string]interface{}{}

	for _, arg := range args {
		marshalled, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}

		if string(marshalled) == "null" || string(marshalled) == "" {
			continue
		}

		if err := json.Unmarshal(marshalled, &result); err != nil {
			return nil, err
		}
	}

	return json.Marshal(result)
}
