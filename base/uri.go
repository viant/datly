package base

import (
	"net/url"
	"strings"
)

//MatchURI parses URIs to match and extract {<param>} defined in rule.URI from request.URI,
//it returns extracted parameters and flag if requestURI matched templateURI
func MatchURI(templateURI, requestURI string) (map[string]string, bool) {
	var expectingValue, expectingName bool
	var name, value string
	var uriParameters = make(map[string]string)
	maxLength := len(templateURI) + len(requestURI)
	var requestURIIndex, templateURIIndex int

	questionMarkPosition := strings.Index(requestURI, "?")
	if questionMarkPosition != -1 {
		requestURI = string(requestURI[:questionMarkPosition])
	}

	for k := 0; k < maxLength; k++ {
		var requestChar, routingChar string
		if requestURIIndex < len(requestURI) {
			requestChar = requestURI[requestURIIndex : requestURIIndex+1]
		}

		if templateURIIndex < len(templateURI) {
			routingChar = templateURI[templateURIIndex : templateURIIndex+1]
		}
		if (!expectingValue && !expectingName) && requestChar == routingChar && routingChar != "" {
			requestURIIndex++
			templateURIIndex++
			continue
		}

		if routingChar == "}" {
			expectingName = false
			templateURIIndex++
		}

		if expectingValue && requestChar == "/" {
			expectingValue = false
		}

		if expectingName && templateURIIndex < len(templateURI) {
			name += routingChar
			templateURIIndex++
		}

		if routingChar == "{" {
			expectingValue = true
			expectingName = true
			templateURIIndex++
		}

		if expectingValue && requestURIIndex < len(requestURI) {
			value += requestChar
			requestURIIndex++
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
	matched := requestURIIndex == len(requestURI) && templateURIIndex == len(templateURI)
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
