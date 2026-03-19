package compile

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/repository/shape"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
)

// RouteIndexEntry maps one source DQL file to one concrete method+URI route key.
type RouteIndexEntry struct {
	RouteKey   string
	Method     string
	URI        string
	SourcePath string
	Namespace  string
}

// RouteIndex stores source-to-route mapping and lookup structures.
type RouteIndex struct {
	ByRouteKey  map[string]*RouteIndexEntry
	ByNamespace map[string][]*RouteIndexEntry
	Conflicts   map[string][]string
}

// BuildRouteIndex scans DQL files and builds route-key mapping.
func BuildRouteIndex(paths []string, opts ...shape.CompileOption) (*RouteIndex, error) {
	compileOptions := applyCompileOptions(opts)
	layout := newCompilePathLayout(compileOptions)
	index := &RouteIndex{
		ByRouteKey:  map[string]*RouteIndexEntry{},
		ByNamespace: map[string][]*RouteIndexEntry{},
		Conflicts:   map[string][]string{},
	}
	if len(paths) == 0 {
		return index, nil
	}
	normalized := make([]string, 0, len(paths))
	for _, item := range paths {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		normalized = append(normalized, item)
	}
	sort.Strings(normalized)

	for _, sourcePath := range normalized {
		data, err := os.ReadFile(sourcePath)
		if err != nil {
			return nil, fmt.Errorf("route index: unable to read %s: %w", sourcePath, err)
		}
		sourceName := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
		dql := string(data)
		_, _, directives, _ := dqlpre.Extract(dql)
		source := &shape.Source{
			Name: sourceName,
			Path: sourcePath,
			DQL:  dql,
		}
		settings := extractRuleSettings(source, directives)
		namespace, _ := dqlToRouteNamespaceWithLayout(sourcePath, layout)
		uri := strings.TrimSpace(settings.URI)
		if uri == "" {
			uri = inferDefaultURI(namespace)
		}
		if uri == "" {
			continue
		}
		methods := parseRouteMethods(settings.Method)
		for _, method := range methods {
			entry := &RouteIndexEntry{
				Method:     method,
				URI:        normalizeURI(uri),
				SourcePath: sourcePath,
				Namespace:  namespace,
			}
			entry.RouteKey = normalizeRouteKey(entry.Method, entry.URI)
			index.addEntry(entry)
		}
	}
	return index, nil
}

// Resolve maps a component reference from current source context to route key.
// It returns false when route cannot be resolved deterministically.
func (r *RouteIndex) Resolve(ref, currentSource string, opts ...shape.CompileOption) (string, bool) {
	if r == nil {
		return "", false
	}
	method, value := splitRouteKey(ref)
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	layout := newCompilePathLayout(applyCompileOptions(opts))
	routeKeyFromURI := func(uri string) (string, bool) {
		key := normalizeRouteKey(method, uri)
		if _, conflicted := r.Conflicts[key]; conflicted {
			return "", false
		}
		if _, ok := r.ByRouteKey[key]; !ok {
			return "", false
		}
		return key, true
	}

	if strings.HasPrefix(value, "/v1/api/") || strings.HasPrefix(value, "v1/api/") || strings.HasPrefix(value, "/") {
		return routeKeyFromURI(value)
	}

	if strings.TrimSpace(currentSource) == "" {
		return "", false
	}
	_, _, dqlRoot, ok := sourceRootsWithLayout(currentSource, layout)
	if !ok {
		return "", false
	}
	sourceNamespace, _ := dqlToRouteNamespaceWithLayout(currentSource, layout)
	namespace := resolveComponentNamespaceWithNamespace(value, currentSource, dqlRoot, sourceNamespace)
	if namespace == "" {
		return "", false
	}
	entries := r.ByNamespace[strings.ToLower(strings.TrimSpace(namespace))]
	if len(entries) == 0 {
		return "", false
	}
	if len(entries) == 1 {
		key := entries[0].RouteKey
		if _, conflicted := r.Conflicts[key]; conflicted {
			return "", false
		}
		return key, true
	}
	// Multiple methods under one namespace: require exact method match.
	for _, candidate := range entries {
		if candidate == nil {
			continue
		}
		if strings.EqualFold(candidate.Method, method) {
			if _, conflicted := r.Conflicts[candidate.RouteKey]; conflicted {
				return "", false
			}
			return candidate.RouteKey, true
		}
	}
	return "", false
}

func (r *RouteIndex) addEntry(entry *RouteIndexEntry) {
	if r == nil || entry == nil {
		return
	}
	key := entry.RouteKey
	if prev, exists := r.ByRouteKey[key]; exists && prev != nil && prev.SourcePath != entry.SourcePath {
		if _, ok := r.Conflicts[key]; !ok {
			r.Conflicts[key] = []string{prev.SourcePath}
		}
		r.Conflicts[key] = append(r.Conflicts[key], entry.SourcePath)
		return
	}
	r.ByRouteKey[key] = entry
	nsKey := strings.ToLower(strings.TrimSpace(entry.Namespace))
	if nsKey != "" {
		r.ByNamespace[nsKey] = append(r.ByNamespace[nsKey], entry)
	}
}

func parseRouteMethods(input string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return []string{http.MethodGet}
	}
	parts := strings.Split(input, ",")
	ret := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		method := strings.ToUpper(strings.TrimSpace(part))
		if method == "" {
			continue
		}
		if seen[method] {
			continue
		}
		seen[method] = true
		ret = append(ret, method)
	}
	if len(ret) == 0 {
		return []string{http.MethodGet}
	}
	return ret
}

func normalizeRouteKey(method, uri string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = http.MethodGet
	}
	return method + ":" + normalizeURI(uri)
}

func normalizeURI(uri string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return "/"
	}
	if strings.HasPrefix(uri, "v1/api/") {
		uri = "/" + uri
	}
	if !strings.HasPrefix(uri, "/") {
		uri = "/" + uri
	}
	return uri
}

func inferDefaultURI(namespace string) string {
	namespace = strings.Trim(strings.TrimSpace(namespace), "/")
	if namespace == "" {
		return ""
	}
	parts := strings.Split(namespace, "/")
	if len(parts) >= 2 && parts[len(parts)-1] == parts[len(parts)-2] {
		parts = parts[:len(parts)-1]
	}
	return "/v1/api/" + strings.Join(parts, "/")
}
