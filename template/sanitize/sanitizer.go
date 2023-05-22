package sanitize

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/view/keywords"
	"strings"
)

func Sanitize(SQL string, hints map[string]*ParameterHint, consts map[string]interface{}) string {
	iterator := NewIterator(SQL, hints, consts)
	offset := 0

	modifiable := []byte(SQL)
	for iterator.Has() {
		paramMeta := iterator.Next()
		if paramMeta.IsVariable && (paramMeta.OccurrenceIndex == 0 && paramMeta.Context == SetContext) {
			continue
		}

		paramName, hadBrackets := unwrapBrackets(paramMeta.FullName)
		sanitized := sanitizeParameter(paramMeta.Context, paramMeta.Prefix, paramMeta.Holder, paramName, iterator.assignedVars, iterator.consts)

		if hadBrackets {
			sanitized = strings.Replace(sanitized, "$", "${", 1) + "}"
		}

		if sanitized == paramMeta.FullName {
			continue
		}

		modifiable = append(modifiable[:offset+paramMeta.Start], bytes.Replace(modifiable[paramMeta.Start+offset:], []byte(paramMeta.FullName), []byte(sanitized), 1)...)
		offset += len(sanitized) - len(paramMeta.FullName)
	}

	return string(modifiable)
}

func unwrapBrackets(name string) (string, bool) {
	if !strings.HasPrefix(name, "${") || !strings.HasSuffix(name, "}") {
		return name, false
	}

	return "$" + name[2:len(name)-1], true
}

func sanitizeParameter(context Context, prefix, paramName, raw string, variables map[string]bool, consts map[string]interface{}) string {
	if fn, ok := keywords.Get(paramName); ok {
		_, ok = fn.Metadata.(*keywords.StandaloneFn)
		if ok {
			return raw
		}
	}

	if prefix == keywords.ParamsMetadataKey {
		return raw
	}

	if _, ok := consts[paramName]; ok {
		return strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1)
	}

	if context == FuncContext || context == ForEachContext || context == IfContext || context == SetContext {
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

	return sanitizeAsPlaceholder(strings.Replace(raw, "$", fmt.Sprintf("$%v.", keywords.ParamsKey), 1))
}

func sanitizeAsPlaceholder(paramName string) string {
	return fmt.Sprintf(" $criteria.AppendBinding(%v)", paramName)
}
