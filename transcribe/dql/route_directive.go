package dql

import (
	"fmt"
	"net/http"
	"strings"
)

type routeDirective struct {
	URI          string
	Methods      []string
	PathParams   []string
	APIKeyHeader string
	APIKeyValue  string
	start        int
	end          int
}

func parseRouteDirective(blocks []directiveBlock) (ret *routeDirective, err error) {
	var active directiveBlock
	defer func() { err = wrapDirectiveError(err, active) }()
	for _, block := range blocks {
		if block.kind != directiveKindSetting {
			continue
		}
		active = block
		name, args, _, ok := parseDirectiveCall(block.body)
		if !ok {
			continue
		}
		switch {
		case strings.EqualFold(name, "route"):
			if ret != nil && ret.URI != "" {
				return nil, fmt.Errorf("invalid route directive: multiple routes are not supported; declare methods in one route")
			}
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid route directive: missing uri")
			}
			uri, ok := parseQuotedLiteral(args[0])
			if !ok || !strings.HasPrefix(strings.TrimSpace(uri), "/") {
				return nil, fmt.Errorf("invalid route directive: uri must be a quoted absolute path")
			}
			pathParams, err := parseRoutePathParams(uri)
			if err != nil {
				return nil, err
			}
			methods, err := normalizeRouteMethods(args[1:])
			if err != nil {
				return nil, err
			}
			if ret == nil {
				ret = &routeDirective{}
			}
			ret.URI = strings.TrimSpace(uri)
			ret.Methods = methods
			ret.PathParams = pathParams
			ret.start = block.start
			ret.end = block.end
		case strings.EqualFold(name, "api_key"):
			if len(args) < 2 {
				return nil, fmt.Errorf("invalid api_key directive: expected header and value")
			}
			if ret == nil {
				ret = &routeDirective{}
			}
			ret.APIKeyHeader = trimQuote(args[0])
			ret.APIKeyValue = trimQuote(args[1])
		}
	}
	if ret != nil && ret.URI != "" && len(ret.Methods) == 0 {
		ret.Methods = []string{"GET"}
	}
	return ret, nil
}

func parseRoutePathParams(uri string) ([]string, error) {
	var result []string
	seen := map[string]bool{}
	for _, segment := range strings.Split(uri, "/") {
		hasOpen := strings.Contains(segment, "{")
		hasClose := strings.Contains(segment, "}")
		if !hasOpen && !hasClose {
			continue
		}
		if len(segment) < 3 || segment[0] != '{' || segment[len(segment)-1] != '}' ||
			strings.Count(segment, "{") != 1 || strings.Count(segment, "}") != 1 {
			return nil, fmt.Errorf("invalid route directive: path placeholder %q must occupy one complete segment", segment)
		}
		name := segment[1 : len(segment)-1]
		if strings.TrimSpace(name) != name || name == "" {
			return nil, fmt.Errorf("invalid route directive: path placeholder name is required")
		}
		if !isIdentifierStart(name[0]) || name[0] == '_' {
			return nil, fmt.Errorf("invalid route directive: path placeholder %q must start with a letter", name)
		}
		for index := 1; index < len(name); index++ {
			if !isIdentifierPart(name[index]) {
				return nil, fmt.Errorf("invalid route directive: path placeholder %q must contain only letters, digits, or underscores", name)
			}
		}
		key := strings.ToLower(name)
		if !seen[key] {
			seen[key] = true
			result = append(result, name)
		}
	}
	return result, nil
}

func normalizeRouteMethods(args []string) ([]string, error) {
	if len(args) == 0 {
		return []string{http.MethodGet}, nil
	}
	valid := map[string]bool{
		http.MethodGet: true, http.MethodPost: true, http.MethodPut: true,
		http.MethodPatch: true, http.MethodDelete: true, http.MethodHead: true,
		http.MethodOptions: true, http.MethodTrace: true, http.MethodConnect: true,
	}
	seen := map[string]bool{}
	methods := make([]string, 0, len(args))
	for _, arg := range args {
		value, ok := parseQuotedLiteral(arg)
		if !ok {
			return nil, fmt.Errorf("invalid route directive: methods must be quoted")
		}
		method := strings.ToUpper(strings.TrimSpace(value))
		if !valid[method] {
			return nil, fmt.Errorf("invalid route directive: unsupported method %q", method)
		}
		if !seen[method] {
			seen[method] = true
			methods = append(methods, method)
		}
	}
	return methods, nil
}
