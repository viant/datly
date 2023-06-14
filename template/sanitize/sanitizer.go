package sanitize

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/view/keywords"
	"strings"
)

func Sanitize(SQL string, hints map[string]*ParameterHint, consts map[string]interface{}) (string, error) {
	iterator, err := NewIterator(SQL, hints, consts, true)
	if err != nil {
		return "", err
	}
	offset := 0

	modifiable := []byte(SQL)
	var paramsBuffer []*ParamMeta
	for {

		next, ok := popNextParamMeta(&paramsBuffer, iterator)
		if !ok {
			break
		}
		modifiable, offset = sanitize(iterator, &paramsBuffer, next, modifiable, offset, 0)
	}

	return strings.TrimSpace(string(modifiable)), nil
}

func popNextParamMeta(buffer *[]*ParamMeta, iterator *ParamMetaIterator) (*ParamMeta, bool) {
	if len(*buffer) > 0 {
		actual := (*buffer)[0]
		*buffer = (*buffer)[1:]
		return actual, true
	}

	has := iterator.Has()
	if has {
		return iterator.Next(), true
	}

	return nil, false
}

func sanitize(iterator *ParamMetaIterator, paramsBuffer *[]*ParamMeta, paramMeta *ParamMeta, dst []byte, accumulatedOffset int, cursorOffset int) ([]byte, int) {
	if paramMeta.IsVariable && (paramMeta.OccurrenceIndex == 0 && paramMeta.Context == SetContext) {
		return dst, accumulatedOffset
	}

	paramExpression, hadBrackets := unwrapBrackets(paramMeta.FullName)
	paramExpression = sanitizeContent(iterator, paramsBuffer, paramMeta, paramExpression)
	sanitized := sanitizeParameter(paramMeta, paramExpression, iterator, dst, accumulatedOffset)

	if hadBrackets {
		sanitized = strings.Replace(sanitized, "$", "${", 1) + "}"
	}

	if sanitized == paramMeta.FullName {
		return dst, accumulatedOffset
	}

	start := paramMeta.Start - cursorOffset
	dataPrefix := dst[:accumulatedOffset+start]
	unsanitizedFragment := dst[start+accumulatedOffset:]
	sanitizedFragment := bytes.Replace(unsanitizedFragment, []byte(paramMeta.FullName), []byte(sanitized), 1)
	dst = append(dataPrefix, sanitizedFragment...)
	accumulatedOffset += (len(sanitized) - len(paramMeta.FullName))
	return dst, accumulatedOffset
}

func sanitizeContent(iterator *ParamMetaIterator, buffer *[]*ParamMeta, meta *ParamMeta, expression string) string {
	var argsParams []*ParamMeta

	for has(iterator, buffer) {
		next := next(iterator, buffer)
		if next.Start < meta.End {
			argsParams = append(argsParams, next)
		} else {
			*buffer = append(*buffer, next)
			break
		}
	}

	if len(argsParams) == 0 {
		return expression
	}

	asBytes := []byte(expression)
	offset := 0
	for _, argParam := range argsParams {
		asBytes, offset = sanitize(iterator, buffer, argParam, asBytes, offset, meta.Start)
	}

	return string(asBytes)
}

func next(iterator *ParamMetaIterator, buffer *[]*ParamMeta) *ParamMeta {
	if len(*buffer) > 0 {
		item := (*buffer)[0]
		*buffer = (*buffer)[1:]
		return item
	}

	return iterator.Next()
}

func has(iterator *ParamMetaIterator, buffer *[]*ParamMeta) bool {
	return len(*buffer) > 0 || iterator.Has()
}

func unwrapBrackets(name string) (string, bool) {
	if !strings.HasPrefix(name, "${") || !strings.HasSuffix(name, "}") {
		return name, false
	}

	return "$" + name[2:len(name)-1], true
}

func sanitizeParameter(paramMeta *ParamMeta, raw string, iterator *ParamMetaIterator, dst []byte, offset int) string {
	context := paramMeta.Context
	prefix := paramMeta.Prefix
	paramName := paramMeta.Holder
	variables := iterator.assignedVars
	consts := iterator.consts

	if fn, ok := keywords.Get(paramName); ok {
		_, ok = fn.Metadata.(*keywords.StandaloneFn)
		if ok {
			return raw
		}
	}

	if prefix == keywords.ParamsMetadataKey {
		return raw
	}

	if paramMeta.Entry != nil {
		_, ok := paramMeta.Entry.Metadata.(*keywords.Namespace)
		if ok {
			return raw
		}
	}

	if _, ok := consts[paramName]; ok {
		return strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1)
	}

	if context == FuncContext || context == ForEachContext || context == IfContext || context == SetContext {
		if paramMeta.Entry != nil {
			return raw
		}

		if variables[paramName] {
			if prefix == keywords.ParamsKey {
				return strings.Replace(raw, fmt.Sprintf("$%v.", keywords.ParamsKey), "$", 1)
			} else {
				return raw
			}
		}

		if prefix == "" {
			return strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1)
		}
		return raw
	}

	isVariable := variables[paramName]
	if isVariable {
		if prefix == keywords.ParamsKey {
			return strings.Replace(raw, "$"+string(keywords.ParamsKey)+".", "$", 1)
		} else {
			return sanitizeAsPlaceholder(raw)
		}
	}

	if prefix == keywords.ParamsKey {
		return raw
	}

	if paramMeta.Entry != nil {
		metadata, ok := paramMeta.Entry.Metadata.(*keywords.ContextMetadata)
		if ok {
			if metadata.UnexpandRaw {
				return raw
			}
		}

		return sanitizeAsPlaceholder(raw)
	}

	return sanitizeAsPlaceholder(strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1))
}

func sanitizeAsPlaceholder(paramName string) string {
	return fmt.Sprintf("$criteria.AppendBinding(%v)", paramName)
}
