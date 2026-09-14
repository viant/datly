package route

import "strings"

func routeIndexKey(method, path string) string {
	return strings.ToUpper(strings.TrimSpace(method)) + " " + strings.TrimSpace(path)
}

func routeTemplateIndexKey(method string, template *PathTemplate) string {
	return strings.ToUpper(strings.TrimSpace(method)) + " " + template.shape()
}
