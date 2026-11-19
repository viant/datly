package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway/router/openapi"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/x/shape"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
	"reflect"
	"strings"
)

func (r *Router) resolveContract(ctx context.Context, request *http.Request, c *repository.Component, method, path string) (*repository.EffectiveContract, error) {
	dynamic, err := c.HasDynamicContract(method, path)
	if err != nil {
		return nil, err
	}
	if !dynamic {
		return &repository.EffectiveContract{}, nil
	}
	if c.View == nil {
		return nil, fmt.Errorf("dynamic contract requires component view binding scope")
	}
	options := append(c.LocatorOptions(request, hstate.NewForm(), c.UnmarshalFunc(request)), locator.WithURIPattern(path), locator.WithLogger(r.logger))
	s := session.New(c.View, session.WithComponent(c), session.WithLogger(r.logger), session.WithLocatorOptions(options...))
	if r.repository != nil {
		s.Apply(session.WithAuth(r.repository.Auth()), session.WithRegistry(r.repository.Registry()))
	}
	if r.operator != nil {
		s.Apply(session.WithOperate(r.operator.Operate))
	}
	return c.ResolveContract(ctx, s.ContractBinder(method, path), method, path)
}

func (r *Router) contractDocumentationContext(ctx context.Context, request *http.Request) context.Context {
	return openapi.WithContractResolver(ctx, func(ctx context.Context, c *repository.Component, target contract.Path) (*repository.EffectiveContract, error) {
		return r.resolveContract(ctx, request, c, target.Method, target.URI)
	})
}

func viewRepresentation(c *repository.Component, effective *repository.EffectiveContract) *shape.Representation {
	if effective == nil || effective.Output == nil {
		return nil
	}
	metadata := effective.Output
	if c.View != nil && c.View.Schema != nil {
		typ := metadata.Original()
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
			typ = typ.Elem()
		}
		viewType := c.View.Schema.Type()
		for viewType != nil && (viewType.Kind() == reflect.Pointer || viewType.Kind() == reflect.Slice) {
			viewType = viewType.Elem()
		}
		if typ == viewType {
			return metadata
		}
	}
	for _, p := range c.Output.Type.Parameters {
		if p.In != nil && p.In.Kind == state.KindOutput && p.In.Name == "view" {
			if member, err := metadata.Lookup(p.Name); err == nil {
				return member
			}
		}
	}
	return nil
}

func projectViewMetadata(doc map[string]any, v *view.View, metadata *shape.Representation) error {
	if metadata == nil || !metadata.Changed() {
		return nil
	}
	for metadata.Original().Kind() == reflect.Pointer || metadata.Original().Kind() == reflect.Slice || metadata.Original().Kind() == reflect.Array {
		metadata = metadata.Elem()
	}
	publicSchema, err := (jsonschema.Reflector{Output: true, ExcludeInternal: true}).Compile(jsonschema.Request{Type: metadata.Original(), Representation: metadata})
	if err != nil {
		return err
	}
	doc["EffectiveSchema"] = publicSchema
	hidden := map[string]bool{}
	byName := map[string]reflect.StructField{}
	for i := 0; i < metadata.NumField(); i++ {
		original := metadata.Original().Field(i)
		field := metadata.Field(i)
		byName[original.Name] = field
		if field.Tag.Get("internal") == "true" || strings.Split(field.Tag.Get("json"), ",")[0] == "-" {
			hidden[original.Name] = true
		}
	}
	if columns, ok := doc["Columns"].([]any); ok {
		kept := make([]any, 0, len(columns))
		for i, column := range columns {
			name := ""
			if i < len(v.Columns) {
				name = v.Columns[i].FieldName()
				if name == "" {
					name = v.Columns[i].Name
				}
			}
			if hidden[name] {
				continue
			}
			if field, ok := byName[name]; ok {
				if item, ok := column.(map[string]any); ok {
					item["Tag"] = string(field.Tag)
				}
			}
			kept = append(kept, column)
		}
		doc["Columns"] = kept
	}
	if configs, ok := doc["ColumnsConfig"].(map[string]any); ok {
		for name := range hidden {
			delete(configs, name)
		}
	}
	if relations, ok := doc["With"].([]any); ok {
		kept := make([]any, 0, len(relations))
		for i, item := range relations {
			if i >= len(v.With) || v.With[i].Of == nil {
				kept = append(kept, item)
				continue
			}
			relation := v.With[i]
			if hidden[relation.Holder] {
				continue
			}
			child, err := metadata.Lookup(relation.Holder)
			if err == nil {
				if data, ok := item.(map[string]any); ok {
					if of, ok := data["Of"].(map[string]any); ok {
						if err = projectViewMetadata(of, &relation.Of.View, child); err != nil {
							return err
						}
					}
				}
			}
			kept = append(kept, item)
		}
		doc["With"] = kept
	}
	return nil
}
