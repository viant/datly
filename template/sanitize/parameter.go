package sanitize

import (
	"github.com/viant/sqlparser"
	"strings"
)

type ParameterHint struct {
	Parameter     string
	Hint          string
	StructQLQuery *StructQLQuery
}

type StructQLQuery struct {
	SQL    string
	Source string
}

func NewParameterHint(name, hint string) *ParameterHint {
	return &ParameterHint{
		Parameter: name,
		Hint:      hint,
	}
}

type ParameterHints []*ParameterHint

func (p *ParameterHints) Index() map[string]*ParameterHint {
	var result = make(map[string]*ParameterHint)
	for i, item := range *p {
		result[item.Parameter] = (*p)[i]
	}

	return result
}

func TryParseStructQLHint(hint string) (*StructQLQuery, bool) {
	_, SQL := SplitHint(hint)
	if SQL == "" {
		return nil, false
	}

	query, err := sqlparser.ParseQuery(SQL)
	if query == nil || query.From.X == nil || err != nil {
		return nil, false
	}

	source := sqlparser.Stringify(query.From.X)
	colonIndex := strings.Index(source, ":")
	if strings.HasPrefix(source, "/") {
		return nil, false
	}
	if colonIndex != -1 {
		source = source[:colonIndex]
	}

	return &StructQLQuery{
		Source: source,
		SQL:    strings.ReplaceAll(SQL, source+":", ""),
	}, true
}
