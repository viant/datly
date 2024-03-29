package parser

import (
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/ast/expr"
	"sort"
	"strings"
)

var auxiliaryPrefixes = []string{keywords.ParamsKey + ".", keywords.SetMarkerKey + "."}

func GetHolderName(identifier string) (string, string) {
	paramName := paramId(identifier)
	prefix, paramName := removePrefixIfNeeded(paramName)
	paramName = withoutPath(paramName)
	return prefix, paramName
}

func splitSelector(selector *expr.Select) (string, string) {
	if selector.X != nil {
		_, ok := selector.X.(*expr.Call)
		if ok {
			return "", selector.ID
		}
	}
	identifier := shared.FirstNotEmpty(selector.FullName, selector.ID)
	identifier = strings.Trim(identifier, "${}")
	if strings.HasPrefix(identifier, "Unsafe.") {
		identifier = identifier[7:]
	}
	var holder, name string
	name = identifier
	//
	if index := strings.LastIndex(identifier, "."); index != -1 {
		nameCandidate := identifier[index+1:]
		if !strings.Contains(nameCandidate, "(") { //method call
			holder = identifier[:index]
			name = nameCandidate
		} else {
			holder = ""
			name = identifier[:index]
		}
	}

	for _, candidate := range auxiliaryPrefixes {
		if strings.HasPrefix(holder, candidate) {
			holder = holder[len(candidate):]
		}
	}
	return holder, name
}

func paramId(identifier string) string {
	if strings.HasPrefix(identifier, "$") {
		identifier = identifier[1:]
	}

	if identifier[0] == '{' {
		identifier = identifier[1 : len(identifier)-1]
	}

	return identifier
}

func GetHolderNameFromSelector(selector *expr.Select) (string, string) {
	if selector.X != nil {
		_, ok := selector.X.(*expr.Call)
		if ok {
			return "", selector.ID
		}
	}

	identifier := shared.FirstNotEmpty(selector.FullName, selector.ID)
	paramName := paramId(identifier)
	prefix, paramName := removePrefixIfNeeded(paramName)
	paramName = withoutPath(paramName)
	return prefix, paramName
}

func removePrefixIfNeeded(name string) (prefix string, actual string) {
	prefixes := []string{
		keywords.AndPrefix, keywords.WherePrefix, keywords.OrPrefix,
		keywords.ParamsKey + ".", keywords.SetMarkerKey + ".",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return prefix[:len(prefix)-1], name[len(prefix):]
		}
	}

	return "", name
}

func withoutPath(name string) string {
	var calls []int

	if index := strings.Index(name, "."); index != -1 {
		calls = append(calls, index)
	}

	if index := strings.Index(name, "["); index != -1 {
		calls = append(calls, index)
	}

	if len(calls) != 0 {
		sort.Ints(calls)
		return name[:calls[0]]
	}

	return name
}
