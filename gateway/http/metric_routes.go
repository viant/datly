package http

import (
	"fmt"
	stdhttp "net/http"
	"net/url"
	"sort"
	"strings"

	rroute "github.com/viant/datly/runtime/route"
)

func (c Config) metricPrefix(input HandlerInput) (string, error) {
	if strings.TrimSpace(c.Meta.MetricURI) == "" {
		return "", nil
	}
	prefix := strings.TrimSuffix(c.Meta.MetricURI, "/")
	parsed, err := url.Parse(prefix)
	if err != nil || !strings.HasPrefix(prefix, "/") || prefix == "" || prefix == "/" || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(prefix, "{}%*\r\n ") {
		return "", fmt.Errorf("invalid metric reporting prefix %q", prefix)
	}
	overlaps := func(path string) bool {
		if strings.TrimSpace(path) == "/" {
			return true
		}
		path = strings.TrimSuffix(strings.TrimSpace(path), "/")
		return path != "" && (path == prefix || strings.HasPrefix(path, prefix+"/") || strings.HasPrefix(prefix, path+"/"))
	}
	for _, reserved := range []string{c.Meta.OpenApiURI, c.Meta.DocURI, c.Meta.CacheWarmURI, c.Meta.CacheInvalidateURI} {
		if overlaps(reserved) {
			return "", fmt.Errorf("metric reporting namespace overlaps metadata route %s", reserved)
		}
	}
	var reporting []*rroute.PathTemplate
	prefixes, err := metricReportingPrefixes(input, prefix+"/")
	if err != nil {
		return "", err
	}
	for _, reportPrefix := range prefixes {
		for _, suffix := range []string{"operations", "counters", "operation/{name}", "counter/{name}", "operation/{name}/recent", "operation/{name}/recent/{metric}", "operation/{name}/cumulative/{metric}"} {
			template, err := rroute.CompilePathTemplate(reportPrefix + suffix)
			if err != nil {
				return "", fmt.Errorf("invalid metric reporting route: %w", err)
			}
			reporting = append(reporting, template)
		}
	}
	for _, endpoint := range input.Runtime.Routes() {
		if overlaps(endpoint.Path) {
			return "", fmt.Errorf("metric reporting namespace collides with component route %s", endpoint.Path)
		}
		component, err := rroute.CompilePathTemplate(endpoint.Path)
		if err != nil {
			return "", fmt.Errorf("metric reporting component route: %w", err)
		}
		for _, template := range reporting {
			intersects, err := metricTemplatesIntersect(component, template)
			if err != nil {
				return "", fmt.Errorf("metric reporting route intersection: %w", err)
			}
			if intersects {
				return "", fmt.Errorf("metric reporting route collides with component route %s", endpoint.Path)
			}
		}
	}
	for _, content := range c.StaticContent {
		if content != nil && overlaps(content.Path) {
			return "", fmt.Errorf("metric reporting namespace collides with static route %s", content.Path)
		}
	}
	return prefix + "/", nil
}

func metricReportingPrefixes(input HandlerInput, prefix string) ([]string, error) {
	if prefix == "" {
		return nil, nil
	}
	result := []string{prefix}
	owner := input.Runtime.Observability()
	if owner != nil && owner.Recorder != nil {
		for _, location := range owner.Recorder.MetricLocations() {
			// A reporting location is a literal relative path. Preserve it rather
			// than cleaning an invalid declaration into a different namespace.
			if location != strings.Trim(location, "/") || strings.ContainsAny(location, "{}%*?#\\\r\n \t") {
				return nil, fmt.Errorf("invalid metric reporting location %q", location)
			}
			for _, segment := range strings.Split(location, "/") {
				if segment == "" || segment == "." || segment == ".." {
					return nil, fmt.Errorf("invalid metric reporting location %q", location)
				}
			}
			result = append(result, prefix+location+"/")
		}
	}
	sort.Slice(result, func(i, j int) bool { return len(result[i]) > len(result[j]) })
	return result, nil
}

// Native templates contain literal or whole-segment nonempty placeholders.
// A common path exists iff lengths agree and every literal pair agrees. Build
// that witness from native canonical escaped segments and verify both with the
// same matcher used for component dispatch, rather than sampling parameter values.
func metricTemplatesIntersect(first, second *rroute.PathTemplate) (bool, error) {
	segments := func(template *rroute.PathTemplate) ([]string, map[string]bool) {
		parameters := map[string]bool{}
		for _, name := range template.Parameters() {
			parameters["{"+name+"}"] = true
		}
		return strings.Split(strings.Trim(template.EscapedTemplatePath(), "/"), "/"), parameters
	}
	left, leftParameters := segments(first)
	right, rightParameters := segments(second)
	if len(left) != len(right) {
		return false, nil
	}
	witness := make([]string, len(left))
	for i := range left {
		switch {
		case !leftParameters[left[i]] && !rightParameters[right[i]]:
			if left[i] != right[i] {
				return false, nil
			}
			witness[i] = left[i]
		case !leftParameters[left[i]]:
			witness[i] = left[i]
		case !rightParameters[right[i]]:
			witness[i] = right[i]
		default:
			witness[i] = "metricRoute"
		}
	}
	path := "/" + strings.Join(witness, "/")
	_, matchesFirst, err := first.MatchEscapedPath(path)
	if err != nil || !matchesFirst {
		return false, err
	}
	_, matchesSecond, err := second.MatchEscapedPath(path)
	return matchesSecond, err
}

func (h *Handler) serveMetricReporting(writer stdhttp.ResponseWriter, request *stdhttp.Request) bool {
	if h.metricPrefix == "" || !strings.HasPrefix(h.routingPath(request), h.metricPrefix) {
		return false
	}
	for _, prefix := range h.metricPrefixes {
		if !strings.HasPrefix(request.URL.EscapedPath(), prefix) {
			continue
		}
		path := strings.TrimPrefix(request.URL.EscapedPath(), prefix)
		parts := strings.Split(path, "/")
		matched := path == "operations" || path == "counters" || len(parts) == 2 && (parts[0] == "counter" || parts[0] == "operation") && parts[1] != "" || len(parts) == 3 && parts[0] == "operation" && parts[1] != "" && parts[2] == "recent" || len(parts) == 4 && parts[0] == "operation" && parts[1] != "" && (parts[2] == "recent" || parts[2] == "cumulative") && parts[3] != ""
		if !matched {
			continue
		}
		if request.Method != stdhttp.MethodGet {
			writer.Header().Set("Allow", stdhttp.MethodGet)
			writer.WriteHeader(stdhttp.StatusMethodNotAllowed)
			return true
		}
		h.runtime.Observability().Recorder.ServeMetrics(prefix, writer, request)
		return true
	}
	return false
}
