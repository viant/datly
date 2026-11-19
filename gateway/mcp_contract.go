package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/jsonrpc"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/viant/datly/repository"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/tagly/choice"
	"github.com/viant/x/shape"
)

func (r *Router) mcpContract(ctx context.Context, c *repository.Component, method, path string) (*repository.EffectiveContract, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost"+path, nil)
	if err != nil {
		return nil, err
	}
	applyMCPHeaders(ctx, req)
	r.addAuthTokenIfPresent(ctx, req)
	return r.resolveContract(ctx, req, c, method, path)
}

func (r *Router) mcpMetadata(ctx context.Context, c *repository.Component, path *dpath.Path, base schema.Tool) (schema.Tool, error) {
	effective, err := r.mcpContract(ctx, c, path.Method, path.URI)
	if err != nil {
		return schema.Tool{}, err
	}
	doc, err := r.mcpInputSchema(c, path, effective)
	if err != nil {
		return schema.Tool{}, err
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return schema.Tool{}, err
	}
	if err = json.Unmarshal(data, &base.InputSchema); err != nil {
		return schema.Tool{}, err
	}
	if c.Output.Type.Schema != nil && c.Output.Type.Schema.Type() != nil {
		doc, err = mcpSchemaReflector(true).Compile(jsonschema.Request{Type: c.Output.Type.Schema.Type(), Representation: effective.Output})
		if err != nil {
			return schema.Tool{}, err
		}
		// The compatibility carrier has a string Type field. Express nullable
		// output roots as an equivalent anyOf while preserving all other keywords.
		if types, ok := doc["type"].([]string); ok {
			alternatives := make([]any, 0, len(types))
			for _, kind := range types {
				alternatives = append(alternatives, map[string]any{"type": kind})
			}
			delete(doc, "type")
			doc["anyOf"] = alternatives
		}
		data, err = json.Marshal(doc)
		if err != nil {
			return schema.Tool{}, err
		}
		base.OutputSchema = &schema.ToolOutputSchema{}
		if err = json.Unmarshal(data, base.OutputSchema); err != nil {
			return schema.Tool{}, err
		}
	}
	return base, nil
}

func (r *Router) contractToolArguments(ctx context.Context, c *repository.Component, route *Route, args map[string]any) (map[string]any, bool, error) {
	effective, err := r.mcpContract(ctx, c, route.Path.Method, route.Path.URI)
	if err != nil {
		return nil, false, err
	}
	typed := c.Input.Type.Schema != nil && c.Input.Type.Schema.Type() != nil
	if effective.Input == nil && (!typed || !choice.HasEnum(c.Input.Type.Schema.Type())) {
		return args, false, nil
	}
	result := make(map[string]any, len(args))
	for name, value := range args {
		result[name] = value
	}
	for _, p := range r.collectToolParameters(c) {
		switch p.In.Kind {
		case state.KindQuery, state.KindPath, state.KindRequestBody, state.KindForm:
		default:
			continue
		}
		field, metadata, hidden, err := mcpParameterField(c, p, effective.Input, false)
		if err != nil {
			return nil, false, err
		}
		if hidden {
			for _, name := range toolArgumentCandidates(p) {
				delete(result, name)
			}
			continue
		}
		visibility := fieldname.Visibility{Representation: metadata, ExcludeInternal: true}
		if isMCPAnonymousBody(p) && indirectType(p.Schema.Type()).Kind() == reflect.Struct {
			normalized, err := fieldname.Normalize(p.Schema.Type(), result, visibility)
			if err != nil {
				return nil, false, err
			}
			result = normalized.(map[string]any)
			continue
		}
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonName == "" {
			jsonName = field.Name
		}
		name, err := fieldname.Resolve(field, jsonName)
		if err != nil {
			return nil, false, err
		}
		candidates := append([]string{jsonName, name.Public}, name.Aliases...)
		seen := map[string]bool{}
		key := ""
		for _, candidate := range candidates {
			if seen[candidate] {
				continue
			}
			seen[candidate] = true
			if _, present := result[candidate]; present {
				if key != "" {
					return nil, false, fmt.Errorf("conflicting MCP names %q and %q", key, candidate)
				}
				key = candidate
			}
		}
		if key == "" {
			continue
		}
		if err := validateMCPEnum(field, result[key]); err != nil {
			return nil, false, err
		}
		normalized, err := fieldname.Normalize(field.Type, result[key], visibility)
		if err != nil {
			return nil, false, err
		}
		delete(result, key)
		result[strings.Title(p.Name)] = normalized
	}
	// Keep inbound enum values and sparse bodies in their declared wire form.
	// The routed HTTP binder owns decoding, presence, codecs and business Init.
	return result, true, nil
}

func effectiveAnonymousBodyArguments(parameter *state.Parameter, args map[string]any, metadata *shape.Representation) (any, error) {
	fields, err := fieldname.Fields(parameter.Schema.Type(), fieldname.Visibility{Representation: metadata, ExcludeInternal: true})
	if err != nil {
		return nil, err
	}
	payload := map[string]any{}
	for _, field := range fields {
		if value, ok := args[field.JSONName]; ok {
			payload[field.JSONName] = value
		}
	}
	if len(payload) == 0 {
		return nil, nil
	}
	return payload, nil
}
func effectiveBodyParameter(parameter *state.Parameter, metadata *shape.Representation) *state.Parameter {
	if metadata == nil || parameter.In == nil || parameter.In.Name == "" {
		return parameter
	}
	for metadata.Original().Kind() == reflect.Pointer {
		metadata = metadata.Elem()
	}
	original, ok := metadata.Original().FieldByName(state.SanitizeTypeName(parameter.In.Name))
	if !ok || len(original.Index) != 1 {
		return parameter
	}
	field := metadata.Field(original.Index[0])
	name := strings.Split(field.Tag.Get("json"), ",")[0]
	if name == "" {
		name = field.Name
	}
	if name == "-" || name == parameter.In.Name {
		return parameter
	}
	copy := *parameter
	location := *parameter.In
	location.Name = name
	copy.In = &location
	return &copy
}

// Invocation policies bind the actual canonical request sources, not a schema
// discovery placeholder. Decoding/initialization still belongs to the HTTP path.
func (r *Router) mcpInvocationContract(ctx context.Context, c *repository.Component, route *Route, args map[string]any) (*repository.EffectiveContract, error) {
	dynamic, err := c.HasDynamicContract(route.Path.Method, route.Path.URI)
	if err != nil {
		return nil, err
	}
	if !dynamic {
		return &repository.EffectiveContract{}, nil
	}
	base := "http://localhost" + route.Path.URI
	query := url.Values{}
	paths, queries := map[string]bool{}, map[string]bool{}
	var body io.Reader
	for _, p := range r.collectToolParameters(c) {
		if p.In == nil {
			continue
		}
		switch p.In.Kind {
		case state.KindQuery, state.KindPath, state.KindRequestBody:
		default:
			continue
		}
		value := toolArgumentValue(p, args)
		if value == nil {
			continue
		}
		var rpcErr *jsonrpc.Error
		base, body, rpcErr = r.applyParamToRequest(base, query, p, value, paths, queries, body, mcpRequestEncoding{preserveEmpty: true, jsonBody: mcpScalarBody(p)})
		if rpcErr != nil {
			return nil, rpcErr
		}
	}
	if encoded := query.Encode(); encoded != "" {
		if strings.Contains(base, "?") {
			base += "&" + encoded
		} else {
			base += "?" + encoded
		}
	}
	request, rpcErr := r.newToolHTTPRequest(ctx, route.Path.Method, base, body)
	if rpcErr != nil {
		return nil, rpcErr
	}
	r.addAuthTokenIfPresent(ctx, request)
	request.RequestURI = request.URL.RequestURI()
	return r.resolveContract(ctx, request, c, route.Path.Method, route.Path.URI)
}

// Consult the current repository generation before resolving policy. A cached
// tool registration must not keep an old component's contract variants alive.
func (r *Router) mcpComponent(ctx context.Context, fallback *repository.Provider, path *dpath.Path) (*repository.Component, error) {
	provider := fallback
	if r.repository != nil {
		var err error
		provider, err = r.repository.Registry().LookupProvider(ctx, &path.Path)
		if err != nil {
			return nil, err
		}
	}
	return provider.Component(ctx)
}

type mcpRequestEncoding struct{ preserveEmpty, jsonBody bool }

func mcpScalarBody(p *state.Parameter) bool {
	if p.In == nil || p.In.Kind != state.KindRequestBody || p.Schema == nil {
		return false
	}
	typ := indirectType(p.Schema.Type())
	if typ == nil {
		return false
	}
	switch typ.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
		return false
	}
	return true
}
