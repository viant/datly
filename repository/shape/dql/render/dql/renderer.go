package dql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/repository/shape/dql/ir"
)

// SourceResolver resolves template SourceURL content when Source is not embedded in IR.
type SourceResolver func(sourceURL string) (string, error)

type options struct {
	rootView string
	resolve  SourceResolver
}

// Option configures DQL rendering.
type Option func(*options)

// WithRootView forces renderer root view selection.
func WithRootView(name string) Option {
	return func(o *options) {
		o.rootView = strings.TrimSpace(name)
	}
}

// WithSourceResolver configures SourceURL content resolution.
func WithSourceResolver(resolver SourceResolver) Option {
	return func(o *options) {
		o.resolve = resolver
	}
}

// Encode renders IR document back to DQL/SQL source for the root route view.
func Encode(doc *ir.Document, opts ...Option) ([]byte, error) {
	if doc == nil || doc.Root == nil {
		return nil, fmt.Errorf("dql render dql: nil IR document")
	}
	cfg := &options{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	views := indexViews(doc.Root)
	if len(views) == 0 {
		return nil, fmt.Errorf("dql render dql: no resource views in IR")
	}
	rootView := cfg.rootView
	if rootView == "" {
		rootView = detectRootView(doc.Root)
	}
	if rootView == "" {
		return nil, fmt.Errorf("dql render dql: unable to detect root route view")
	}
	view := views[rootView]
	if view == nil {
		return nil, fmt.Errorf("dql render dql: root view %q not found in resources", rootView)
	}
	sql, err := renderViewSQL(view, cfg)
	if err != nil {
		return nil, err
	}
	return []byte(strings.TrimSpace(sql) + "\n"), nil
}

func renderViewSQL(view map[string]any, cfg *options) (string, error) {
	name := stringValue(view["Name"])
	template := mapValue(view["Template"])
	if template == nil {
		return "", fmt.Errorf("dql render dql: view %q has no template", name)
	}
	if source := strings.TrimSpace(stringValue(template["Source"])); source != "" {
		return source, nil
	}
	sourceURL := strings.TrimSpace(stringValue(template["SourceURL"]))
	if sourceURL == "" {
		return "", fmt.Errorf("dql render dql: view %q has neither template source nor sourceURL", name)
	}
	source, err := resolveSourceURL(cfg, view, sourceURL)
	if err != nil {
		return "", fmt.Errorf("dql render dql: view %q resolve %q failed: %w", name, sourceURL, err)
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return "", fmt.Errorf("dql render dql: resolved source was empty for %q", sourceURL)
	}
	return source, nil
}

func resolveSourceURL(cfg *options, view map[string]any, sourceURL string) (string, error) {
	_ = view
	if cfg.resolve != nil {
		return cfg.resolve(sourceURL)
	}
	return "", fmt.Errorf("requires SourceURL resolver for %q", sourceURL)
}

func detectRootView(root map[string]any) string {
	for _, routeItem := range sliceValue(root["Routes"]) {
		route := mapValue(routeItem)
		if route == nil {
			continue
		}
		view := mapValue(route["View"])
		if view == nil {
			continue
		}
		if ref := strings.TrimSpace(stringValue(view["Ref"])); ref != "" {
			return ref
		}
	}
	return ""
}

func indexViews(root map[string]any) map[string]map[string]any {
	result := map[string]map[string]any{}
	resource := mapValue(root["Resource"])
	if resource == nil {
		return result
	}
	for _, item := range sliceValue(resource["Views"]) {
		view := mapValue(item)
		if view == nil {
			continue
		}
		name := strings.TrimSpace(stringValue(view["Name"]))
		if name == "" {
			continue
		}
		result[name] = view
	}
	return result
}

func mapValue(raw any) map[string]any {
	if v, ok := raw.(map[string]any); ok {
		return v
	}
	if v, ok := raw.(map[any]any); ok {
		out := map[string]any{}
		for key, item := range v {
			out[fmt.Sprint(key)] = item
		}
		return out
	}
	return nil
}

func sliceValue(raw any) []any {
	if items, ok := raw.([]any); ok {
		return items
	}
	return nil
}

func stringValue(raw any) string {
	if raw == nil {
		return ""
	}
	if value, ok := raw.(string); ok {
		return value
	}
	return fmt.Sprint(raw)
}
