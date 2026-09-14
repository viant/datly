package developer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	afsembed "github.com/viant/afs/embed"
	"github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/x/shape"
)

type ComponentSummary struct {
	ID     string        `json:"id"`
	Name   string        `json:"name"`
	Routes []*spec.Route `json:"routes"`
}
type ComponentList struct {
	Target     string             `json:"target"`
	Context    string             `json:"context"`
	Revision   string             `json:"revision"`
	Components []ComponentSummary `json:"components"`
}
type Inspection struct {
	Target      string         `json:"target"`
	Context     string         `json:"context"`
	Revision    string         `json:"revision"`
	Component   map[string]any `json:"component"`
	MetadataURI string         `json:"metadataUri,omitempty"`
}
type sourceSnapshot struct {
	components     []*spec.Component
	originals      map[string]string
	revision, kind string
	shapes         map[string]map[string]any
}

func (s *Service) snapshot(ctx context.Context, args arguments) (sourceSnapshot, error) {
	if args.InstanceID != "" {
		s.mu.Lock()
		instance := s.instances[args.InstanceID]
		s.mu.Unlock()
		if instance == nil || instance.state.Target != args.Target {
			return sourceSnapshot{}, fmt.Errorf("unknown owned target instance")
		}
		metadata, err := instance.server.Metadata(ctx)
		if err != nil {
			return sourceSnapshot{}, err
		}
		return sourceSnapshot{components: metadata.Components, kind: "application", revision: fmt.Sprint(metadata.Revision)}, nil
	}
	validator := s.targets[args.Target]
	discovery := transcribe.Discovery{BaseDir: validator.BaseDir, Include: validator.Include, Exclude: validator.Exclude, ModuleDirs: validator.ModuleDirs, Connector: validator.Connector, ColumnRefiner: validator.ColumnRefiner}
	if app, ok := s.applications[args.Target]; ok {
		discovery.Registry = app.Options.Registry
	}
	project, err := discovery.Compile(ctx)
	if err != nil {
		return sourceSnapshot{}, err
	}
	result := sourceSnapshot{kind: "project", originals: map[string]string{}, shapes: map[string]map[string]any{}}
	for _, compiled := range project.Components {
		result.components = append(result.components, compiled.Component.Clone())
		result.originals[compiled.Component.Key.String()] = compiled.Source.Text
		if compiled.TypeResolver != nil && compiled.Component.Settings != nil {
			contracts := map[string]any{}
			for role, expression := range map[string]string{"input": compiled.Component.Settings.InputType, "output": compiled.Component.Settings.OutputType} {
				if expression == "" {
					continue
				}
				descriptor, err := compiled.TypeResolver.Descriptor(expression)
				if err != nil {
					return result, err
				}
				if descriptor == nil {
					continue
				}
				fields, err := shape.New(descriptor, compiled.TypeResolver.Descriptor).Fields()
				if err != nil {
					return result, err
				}
				var descriptions []map[string]string
				for _, field := range fields {
					if field.Exported {
						descriptions = append(descriptions, map[string]string{"name": field.Name, "type": field.TypeExpr, "tag": string(field.Tag)})
					}
				}
				contracts[role] = map[string]any{"type": expression, "fields": descriptions}
			}
			result.shapes[compiled.Component.Key.String()] = contracts
		}
	}
	data, err := json.Marshal(result.components)
	if err != nil {
		return result, err
	}
	result.revision = fmt.Sprintf("%x", sha256.Sum256(data))
	return result, nil
}

func (s *Service) inspect(ctx context.Context, args arguments, operation string) (any, error) {
	snapshot, err := s.snapshot(ctx, args)
	if err != nil {
		return nil, err
	}
	listing := &ComponentList{Target: args.Target, Context: snapshot.kind, Revision: snapshot.revision, Components: []ComponentSummary{}}
	for _, component := range snapshot.components {
		for _, route := range component.Routes {
			route.APIKeyValue = ""
		}
		id := component.Key.String()
		if operation == ComponentsTool {
			if strings.HasPrefix(id, args.Prefix) {
				listing.Components = append(listing.Components, ComponentSummary{ID: id, Name: component.Name, Routes: component.Routes})
			}
			continue
		}
		if id != args.Component {
			continue
		}
		if operation == ReverseTool {
			return (dql.Serializer{}).Export(component, snapshot.originals[id]), nil
		}
		data, err := json.Marshal(component)
		if err != nil {
			return nil, err
		}
		result := &Inspection{Target: args.Target, Context: snapshot.kind, Revision: snapshot.revision}
		if err = json.Unmarshal(data, &result.Component); err != nil {
			return nil, err
		}
		if shapes := snapshot.shapes[id]; shapes != nil {
			result.Component["contractShapes"] = shapes
			data, err = json.Marshal(result.Component)
			if err != nil {
				return nil, err
			}
		}
		// Scoped tool-only authorization must not turn into public metadata resources.
		// Global policy protects both surfaces; otherwise retain the full inline result.
		if s.policy == nil || s.policy.Global != nil {
			result.MetadataURI, err = s.document(ctx, args.Target, data)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	if operation == ComponentsTool {
		return listing, nil
	}
	return nil, fmt.Errorf("component was not found in the selected generation")
}

func (s *Service) document(ctx context.Context, target string, data []byte) (string, error) {
	id := fmt.Sprintf("%x", sha256.Sum256(append([]byte(target), data...)))
	prefix := "datly-meta://components/" + id
	uri := prefix + "/metadata.json"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.documents == nil {
		s.documents = map[string]*resource.Plan{}
	}
	if s.documents[uri] != nil {
		return uri, nil
	}
	if len(s.documents) >= 64 {
		return "", nil
	}
	holder := afsembed.NewHolder()
	holder.Add("metadata.json", string(data))
	plans, err := (resource.Publisher{}).Compile(ctx, []resource.Folder{{Namespace: id, URIPrefix: prefix, FS: holder.EmbedFs()}})
	if err != nil {
		return "", err
	}
	s.documents[uri] = plans[0]
	all := append([]*resource.Plan(nil), s.bundled...)
	for _, plan := range s.documents {
		all = append(all, plan)
	}
	catalog, err := resource.NewCatalog(all)
	if err != nil {
		return "", err
	}
	s.resources = resource.NewHandler(catalog, nil)
	for _, metadata := range catalog.Resources() {
		s.registry.RegisterResource(metadata, s.resources.Handle)
	}
	return uri, nil
}
