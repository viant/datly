package docs

import (
	"context"
	"fmt"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	xdocs "github.com/viant/xdatly/docs"
	"gopkg.in/yaml.v3"
	"io"
	"path"
	"strings"
)

// Loader reads only through the stage's canonical Bindly store. External AFS
// filesystems can be registered using afs/adapter/io.NewFS, just like embed.FS.
type Loader struct{ Resources *resource.Store }

// Snapshot is immutable after Load. Annotation lookups never fetch resources.
type Snapshot struct {
	lineage                            map[string]*sqlparser.ColumnLineage
	schemas                            map[string]string
	schemaResources                    []string
	responses                          map[string]string
	filter, columns, parameters, paths dictionary
	origins                            []string
	views                              map[string]*spec.View
}

func (s *Snapshot) Origins() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.origins...)
}
func (l Loader) Load(ctx context.Context, sources ...xdocs.Source) (*Snapshot, error) {
	return l.Overlay(ctx, nil, sources...)
}

// Overlay extends immutable resolved annotations. A shared global snapshot is
// loaded once for a generation, not fetched again per component or publisher.
func (l Loader) Overlay(ctx context.Context, base *Snapshot, sources ...xdocs.Source) (*Snapshot, error) {
	result := &Snapshot{schemas: map[string]string{}, responses: map[string]string{}, filter: dictionary{}, columns: dictionary{}, parameters: dictionary{}, paths: dictionary{}}
	if base != nil {
		result.filter.merge(base.filter)
		result.columns.merge(base.columns)
		result.parameters.merge(base.parameters)
		result.paths.merge(base.paths)
		for key, value := range base.responses {
			result.responses[key] = value
		}
		for key, value := range base.schemas {
			result.schemas[key] = value
		}
		result.origins = append(result.origins, base.origins...)
		result.schemaResources = append(result.schemaResources, base.schemaResources...)
	}
	for _, source := range sources {
		refs := source.DocURLs
		if len(refs) == 0 && source.DocURL != "" {
			refs = []string{source.DocURL}
		}
		refs = append(append([]string(nil), source.GlobalURLs...), refs...)
		for _, ref := range refs {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if strings.TrimSpace(ref) == "" {
				return nil, fmt.Errorf("empty documentation reference")
			}
			if !strings.Contains(ref, ":") && source.BaseURL != "" {
				base := strings.TrimRight(source.BaseURL, "/")
				if strings.HasSuffix(base, ":") {
					ref = base + ref
				} else {
					ref = base + "/" + ref
				}
			}
			if l.Resources == nil {
				return nil, fmt.Errorf("documentation %s: resource store required", ref)
			}
			data, err := l.Resources.ReadFile(ref)
			if err != nil {
				return nil, fmt.Errorf("documentation %s: %w", ref, err)
			}
			if ext := strings.ToLower(path.Ext(ref)); ext != ".yaml" && ext != ".yml" && ext != ".json" {
				return nil, fmt.Errorf("documentation %s: expected YAML or JSON", ref)
			}
			decoder := yaml.NewDecoder(strings.NewReader(source.Expand(string(data))))
			var node yaml.Node
			if err = decoder.Decode(&node); err != nil {
				return nil, fmt.Errorf("documentation %s: %w", ref, err)
			}
			var trailing yaml.Node
			if err = decoder.Decode(&trailing); err != io.EOF {
				return nil, fmt.Errorf("documentation %s: expected one document", ref)
			}
			if len(node.Content) != 1 {
				return nil, fmt.Errorf("documentation %s: empty document", ref)
			}
			sections := node.Content[0]
			if sections.Kind != yaml.MappingNode {
				return nil, fmt.Errorf("documentation %s: expected dictionary", ref)
			}
			seen := map[string]bool{}
			for i := 0; i < len(sections.Content); i += 2 {
				key := strings.ToLower(sections.Content[i].Value)
				value := sections.Content[i+1]
				if seen[key] {
					return nil, fmt.Errorf("documentation %s: duplicate section %s", ref, key)
				}
				seen[key] = true
				if key == "responses" {
					if err = result.decodeResponses(value, l.Resources, ref); err != nil {
						return nil, fmt.Errorf("documentation %s: %w", ref, err)
					}
					continue
				}
				var target dictionary
				switch key {
				case "filter":
					target = result.filter
				case "columns":
					target = result.columns
				case "parameters":
					target = result.parameters
				case "paths":
					target = result.paths
				default:
					return nil, fmt.Errorf("documentation %s: unknown section %q", ref, key)
				}
				doc := dictionary{}
				if err = doc.decode(value); err != nil {
					return nil, fmt.Errorf("documentation %s: %w", ref, err)
				}
				target.merge(doc)
			}
			result.origins = append(result.origins, ref)
		}
	}
	return result, nil
}
