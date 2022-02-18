package config

import (
	"net/url"
	"strings"
)

//MatchPath parses Paths to match and extract {<param>} defined in rule.Path from request.Path,
//it returns extracted parameters and flag if requestPath matched templatePath
func MatchPath(templatePath, requestPath string) (map[string]string, bool) {
	var expectingValue, expectingName bool
	var name, value string
	var uriParameters = make(map[string]string)
	maxLength := len(templatePath) + len(requestPath)
	var requestPathIndex, templatePathIndex int

	questionMarkPosition := strings.Index(requestPath, "?")
	if questionMarkPosition != -1 {
		requestPath = string(requestPath[:questionMarkPosition])
	}

	for k := 0; k < maxLength; k++ {
		var requestChar, routingChar string
		if requestPathIndex < len(requestPath) {
			requestChar = requestPath[requestPathIndex : requestPathIndex+1]
		}

		if templatePathIndex < len(templatePath) {
			routingChar = templatePath[templatePathIndex : templatePathIndex+1]
		}
		if (!expectingValue && !expectingName) && requestChar == routingChar && routingChar != "" {
			requestPathIndex++
			templatePathIndex++
			continue
		}

		if routingChar == "}" {
			expectingName = false
			templatePathIndex++
		}

		if expectingValue && requestChar == "/" {
			expectingValue = false
		}

		if expectingName && templatePathIndex < len(templatePath) {
			name += routingChar
			templatePathIndex++
		}

		if routingChar == "{" {
			expectingValue = true
			expectingName = true
			templatePathIndex++
		}

		if expectingValue && requestPathIndex < len(requestPath) {
			value += requestChar
			requestPathIndex++
		}

		if !expectingValue && !expectingName && len(name) > 0 {
			uriParameters[name] = value
			name = ""
			value = ""
		}

	}

	if len(name) > 0 && len(value) > 0 {
		uriParameters[name] = value
	}
	matched := requestPathIndex == len(requestPath) && templatePathIndex == len(templatePath)
	return uriParameters, matched
}

//MergeValues merge values
func MergeValues(values url.Values, target map[string]interface{}) {
	if len(values) == 0 {
		return
	}
	for k, v := range values {
		if len(v) == 1 {
			target[k] = values.Get(k)
			continue
		}
		target[k] = v
	}
}

//MergeMap merge values
func MergeMap(values map[string]interface{}, target map[string]interface{}) {
	if len(values) == 0 {
		return
	}
	for k, v := range values {
		target[k] = v
	}
}
