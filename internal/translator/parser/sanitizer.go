package parser

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"strings"
)

func (t *Template) Sanitize() string {
	SQL := t.SQL
	iterable := t.iterable()
	offset := 0
	modifiable := []byte(SQL)
	for {
		next, ok := iterable.Pop()
		if !ok {
			break
		}
		nextText := string(SQL[next.Start:next.End])
		fmt.Printf("next %s\n", nextText)
		modifiable, offset = sanitize(iterable, next, modifiable, offset, 0)
	}
	return strings.TrimSpace(string(modifiable))
}

func sanitize(iterable *iterables, expression *Expression, dst []byte, accumulatedOffset int, cursorOffset int) ([]byte, int) {
	if expression.IsVariable && (expression.OccurrenceIndex == 0 && expression.Context == SetContext) {
		return dst, accumulatedOffset
	}
	paramExpression, hadBrackets := unwrapBrackets(expression.FullName)
	paramExpression = sanitizeContent(iterable, expression, paramExpression)
	sanitized := sanitizeParameter(expression, paramExpression, iterable, dst, accumulatedOffset)

	if hadBrackets {
		sanitized = strings.Replace(sanitized, "$", "${", 1) + "}"
	}
	if sanitized == expression.FullName {
		return dst, accumulatedOffset
	}
	start := expression.Start - cursorOffset
	dataPrefix := dst[:accumulatedOffset+start]
	unsanitizedFragment := dst[start+accumulatedOffset:]
	sanitizedFragment := bytes.Replace(unsanitizedFragment, []byte(expression.FullName), []byte(sanitized), 1)
	dst = append(dataPrefix, sanitizedFragment...)
	accumulatedOffset += (len(sanitized) - len(expression.FullName))
	return dst, accumulatedOffset
}

func sanitizeContent(iterator *iterables, meta *Expression, expression string) string {
	var argsParams []*Expression
	for iterator.Has() {
		next := iterator.Next()
		if next.Start < meta.End {
			argsParams = append(argsParams, next)
		} else {
			iterator.Push(next)
			break
		}
	}
	if len(argsParams) == 0 {
		return expression
	}
	asBytes := []byte(expression)
	offset := 0
	for _, argParam := range argsParams {
		asBytes, offset = sanitize(iterator, argParam, asBytes, offset, meta.Start)
	}
	return string(asBytes)
}

func unwrapBrackets(name string) (string, bool) {
	if !strings.HasPrefix(name, "${") || !strings.HasSuffix(name, "}") {
		return name, false
	}

	return "$" + name[2:len(name)-1], true
}

func sanitizeParameter(expression *Expression, raw string, iterator *iterables, dst []byte, offset int) string {
	context := expression.Context
	prefix := expression.Prefix
	paramName := expression.Holder
	variables := iterator.Declared

	if fn, ok := keywords.Get(paramName); ok {
		_, ok = fn.Metadata.(*keywords.StandaloneFn)
		if ok {
			return raw
		}
	}

	if prefix == keywords.ParamsMetadataKey {
		return raw
	}
	if expression.Entry != nil {
		_, ok := expression.Entry.Metadata.(*keywords.Namespace)
		if ok {
			return raw
		}
	}

	if param := iterator.State.Lookup(paramName); param != nil {
		if param.In != nil && param.In.Kind == view.KindLiteral {
			return strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1)
		}
	}

	if context == FuncContext || context == ForEachContext || context == IfContext || context == SetContext {
		if expression.Entry != nil {
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

	if expression.Entry != nil {
		metadata, ok := expression.Entry.Metadata.(*keywords.ContextMetadata)
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
