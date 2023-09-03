package shared

import (
	"net/http"
	"strings"
)

func ExtractPath(name string) (string, string) {
	pair := strings.Split(name, ":")
	method := http.MethodGet
	URI := ""
	switch len(pair) {
	case 1:
		URI = name
	case 2:
		method = pair[0]
		URI = pair[1]
	}
	return method, URI
}
