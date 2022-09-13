package sanitizer

import (
	"github.com/viant/datly/view/keywords"
	"strings"
)

func GetHolderName(identifier string) (string, string) {
	paramName := paramId(identifier)
	prefix, paramName := removePrefixIfNeeded(paramName)
	paramName = withoutPath(paramName)
	return prefix, paramName
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

func removePrefixIfNeeded(name string) (prefix string, actual string) {
	prefixes := []string{
		keywords.AndPrefix, keywords.WherePrefix, keywords.OrPrefix,
		keywords.ParamsKey + ".", keywords.ParamsMetadataKey + ".", Const + ".",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return prefix[:len(prefix)-1], name[len(prefix):]
		}
	}

	return "", name
}

func withoutPath(name string) string {
	if index := strings.Index(name, "."); index != -1 {
		return name[:index]
	}

	return name
}
