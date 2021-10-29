package router

import (
	"github.com/viant/datly/oas/spec"
	"strings"
)

//Match parses path to match and extract {<param>},
//it returns extracted parameters and flag if requestPath matched templatePath
func Match(templatePath, requestPath string) ([]*spec.Value, bool) {
	var expectingValue, expectingName bool
	var name, value string
	var uriParameters = make([]*spec.Value, 0)
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
			uriParameters = append(uriParameters, spec.NewValue("path", name, value))
			name = ""
			value = ""
		}
	}

	if len(name) > 0 && len(value) > 0 {
		uriParameters = append(uriParameters, spec.NewValue("path", name, value))
	}
	matched := requestPathIndex == len(requestPath) && templatePathIndex == len(templatePath)
	return uriParameters, matched
}
