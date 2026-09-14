package spec

import (
	"fmt"
	"strings"
)

// RouteRef is the canonical identity stored by a component binding source.
type RouteRef struct {
	Method string
	Path   string
}

func ParseRouteRef(value string) (RouteRef, error) {
	value = strings.TrimSpace(value)
	separator := strings.IndexByte(value, ':')
	if separator <= 0 {
		return RouteRef{}, fmt.Errorf("component route reference %q must use METHOD:/path", value)
	}
	ref := RouteRef{
		Method: strings.ToUpper(strings.TrimSpace(value[:separator])),
		Path:   strings.TrimSpace(value[separator+1:]),
	}
	if ref.Method == "" || ref.Path == "" || !strings.HasPrefix(ref.Path, "/") {
		return RouteRef{}, fmt.Errorf("component route reference %q must use METHOD:/path", value)
	}
	return ref, nil
}

func (r RouteRef) String() string {
	method := strings.ToUpper(strings.TrimSpace(r.Method))
	path := strings.TrimSpace(r.Path)
	if method == "" || path == "" {
		return ""
	}
	return method + ":" + path
}
