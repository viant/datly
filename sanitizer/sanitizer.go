package sanitizer

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/functions"
	"strings"
)

var builtInMethods = map[string]bool{
	view.Logger:            true,
	view.Criteria:          true,
	keywords.ParentViewKey: true,
	keywords.ViewKey:       true,
	functions.SlicesFunc:   true,
	functions.MathFunc:     true,
	functions.TimeFunc:     true,
	functions.StringsFunc:  true,
	functions.ErrorsFunc:   true,
	functions.StrconvFunc:  true,
	functions.TypesFunc:    true,
}

func Sanitize(SQL string, hints option.ParameterHints) string {
	iterator := NewIterator(SQL, hints)
	offset := 0

	modifiable := []byte(SQL)
	for iterator.Has() {
		paramMeta := iterator.Next()
		if paramMeta.IsVariable && paramMeta.OccurrenceIndex == 0 {
			continue
		}

		sanitized := sanitizeParameter(paramMeta.Context, paramMeta.Prefix, paramMeta.Holder, paramMeta.FullName, iterator.variables)
		if sanitized == paramMeta.FullName {
			continue
		}

		modifiable = append(modifiable[:offset+paramMeta.Start], bytes.Replace(modifiable[paramMeta.Start+offset:], []byte(paramMeta.FullName), []byte(sanitized), 1)...)
		offset += len(sanitized) - len(paramMeta.FullName)
	}

	return string(modifiable)
}

func sanitizeParameter(context Context, prefix, paramName, raw string, variables map[string]bool) string {
	if prefix == keywords.ParamsMetadataKey {
		return raw
	}

	if (context == FuncContext || context == ForEachContext || context == IfContext) && variables[paramName] {
		return strings.Replace(raw, fmt.Sprintf("$%v.", keywords.ParamsKey), "$", 1)
	}

	isVariable := variables[paramName]
	if isVariable {
		if prefix == keywords.ParamsKey {
			return strings.Replace(raw, "$"+string(keywords.ParamsKey), "$", 1)
		} else {
			return sanitizeAsPlaceholder(raw)
		}
	}

	if prefix == keywords.ParamsKey {
		return raw
	}

	return sanitizeAsPlaceholder(strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1))
}

func sanitizeAsPlaceholder(paramName string) string {
	return fmt.Sprintf(" $criteria.AppendBinding(%v) ", paramName)
}
