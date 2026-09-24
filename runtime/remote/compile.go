package remote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/viant/datly/transform"
)

const (
	targetHeader   = "header"
	targetQuery    = "query"
	targetPath     = "path"
	targetBody     = "body"
	targetArgument = "argument"
)

var placeholderPattern = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*)\}`)

type compileInput struct {
	inputType, outputType reflect.Type
	hasResources          bool
}

type inputSelector struct {
	path string
	plan *transform.Path
}

type requestBinding struct {
	input  inputSelector
	target string
	name   string
	body   *transform.Path
}

type credentialBinding struct {
	config Credential
}

type validityBinding struct {
	path   *transform.Path
	format string
}

// plan is one validated configuration bound to concrete input/output types.
type plan struct {
	identity     string
	transport    string
	url          string
	method       string
	tool         string
	placeholders map[string]bool
	staticHeader map[string]string
	source       string
	request      []requestBinding
	response     *transform.Plan
	credential   *credentialBinding
	ttl          time.Duration
	partition    []inputSelector
	validity     *validityBinding
	cacheName    string
	inputType    reflect.Type
	outputType   reflect.Type
}

// configIdentity is the stable digest of the complete declaration; compiled
// plans and cache keys are scoped by it.
func configIdentity(config *Config) (string, error) {
	encoded, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("encode declaration: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func compile(config *Config, in compileInput) (*plan, error) {
	if config == nil {
		return nil, fmt.Errorf("remote configuration is required")
	}
	if in.inputType == nil || in.inputType.Kind() != reflect.Struct || in.outputType == nil || in.outputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("remote handler requires struct input and output types")
	}
	identity, err := configIdentity(config)
	if err != nil {
		return nil, err
	}
	result := &plan{
		identity:   identity,
		transport:  config.Client.Transport,
		inputType:  in.inputType,
		outputType: in.outputType,
	}
	if err := result.compileClient(&config.Client); err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	if err := result.compileRequest(config.Request); err != nil {
		return nil, err
	}
	if err := result.compileCredential(config.Credential, in.hasResources); err != nil {
		return nil, err
	}
	if err := result.compileResponse(config.Response); err != nil {
		return nil, err
	}
	if err := result.compileCache(config.Cache, config.Response.Validity); err != nil {
		return nil, err
	}
	return result, nil
}

func (p *plan) compileClient(options *ClientOptions) error {
	if err := options.Validate(); err != nil {
		return err
	}
	var err error
	p.placeholders = map[string]bool{}
	switch options.Transport {
	case TransportHTTP:
		p.url, p.method = options.HTTP.URL, options.HTTP.Method
		for _, match := range placeholderPattern.FindAllStringSubmatch(options.HTTP.URL, -1) {
			p.placeholders[match[1]] = false
		}
		p.staticHeader, err = canonicalHeaders(options.HTTP.Header)
	default:
		p.url, p.tool = options.MCP.URL, options.MCP.Tool
		p.staticHeader, err = canonicalHeaders(options.MCP.Header)
	}
	return err
}

func (p *plan) lookupInput(path string) (inputSelector, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return inputSelector{}, fmt.Errorf("input path is required")
	}
	selector, err := transform.CompileSelector(path)
	if err != nil {
		return inputSelector{}, err
	}
	selector, err = selector.ForType(p.inputType)
	if err != nil {
		return inputSelector{}, err
	}
	return inputSelector{path: path, plan: selector}, nil
}

func (p *plan) compileRequest(mappings []RequestMapping) error {
	headers := map[string]bool{}
	queries := map[string]bool{}
	arguments := map[string]bool{}
	bodies := map[string]bool{}
	for index, mapping := range mappings {
		input, err := p.lookupInput(mapping.Input)
		if err != nil {
			return fmt.Errorf("request[%d]: %w", index, err)
		}
		binding := requestBinding{input: input}
		targets := 0
		if mapping.Header != "" {
			targets++
			binding.target, binding.name = targetHeader, http.CanonicalHeaderKey(strings.TrimSpace(mapping.Header))
		}
		if mapping.Query != "" {
			targets++
			binding.target, binding.name = targetQuery, mapping.Query
		}
		if mapping.Path != "" {
			targets++
			binding.target, binding.name = targetPath, mapping.Path
		}
		if mapping.Body != nil {
			targets++
			binding.target = targetBody
			binding.body, err = transform.CompilePointer(*mapping.Body)
			if err != nil {
				return fmt.Errorf("request[%d] body: %w", index, err)
			}
			binding.name = binding.body.String()
		}
		if mapping.Argument != "" {
			targets++
			binding.target, binding.name = targetArgument, mapping.Argument
		}
		if targets != 1 {
			return fmt.Errorf("request[%d] for input %q must declare exactly one of header, query, path, body or argument", index, mapping.Input)
		}
		switch binding.target {
		case targetArgument:
			if p.transport != TransportMCP {
				return fmt.Errorf("request[%d]: argument targets require transport %q", index, TransportMCP)
			}
			if arguments[binding.name] {
				return fmt.Errorf("request[%d]: argument %q is mapped more than once", index, binding.name)
			}
			arguments[binding.name] = true
		case targetHeader:
			if headers[binding.name] || p.staticHeader[binding.name] != "" {
				return fmt.Errorf("request[%d]: header %q is declared more than once", index, binding.name)
			}
			headers[binding.name] = true
		case targetQuery:
			if p.transport != TransportHTTP {
				return fmt.Errorf("request[%d]: query targets require transport %q", index, TransportHTTP)
			}
			if queries[binding.name] {
				return fmt.Errorf("request[%d]: query %q is mapped more than once", index, binding.name)
			}
			queries[binding.name] = true
		case targetPath:
			if p.transport != TransportHTTP {
				return fmt.Errorf("request[%d]: path targets require transport %q", index, TransportHTTP)
			}
			filled, ok := p.placeholders[binding.name]
			if !ok {
				return fmt.Errorf("request[%d]: url has no {%s} placeholder", index, binding.name)
			}
			if filled {
				return fmt.Errorf("request[%d]: placeholder {%s} is mapped more than once", index, binding.name)
			}
			p.placeholders[binding.name] = true
		case targetBody:
			if p.transport != TransportHTTP {
				return fmt.Errorf("request[%d]: body targets require transport %q", index, TransportHTTP)
			}
			if p.method == http.MethodGet || p.method == http.MethodDelete {
				return fmt.Errorf("request[%d]: http method %s cannot carry a body mapping", index, p.method)
			}
			if bodies[binding.name] || (len(bodies) > 0 && (binding.name == "" || bodies[""])) {
				return fmt.Errorf("request[%d]: body location %q conflicts with another body mapping", index, binding.name)
			}
			bodies[binding.name] = true
		}
		p.request = append(p.request, binding)
	}
	for name, filled := range p.placeholders {
		if !filled {
			return fmt.Errorf("url placeholder {%s} has no path request mapping", name)
		}
	}
	return nil
}

func (p *plan) compileCredential(config *Credential, hasResources bool) error {
	if config == nil {
		return nil
	}
	binding := &credentialBinding{config: *config}
	if strings.TrimSpace(config.Resource) == "" {
		return fmt.Errorf("credential requires a resource reference")
	}
	if (config.Header == "") == (config.Argument == "") {
		return fmt.Errorf("credential must declare exactly one of header or argument")
	}
	if !hasResources {
		return fmt.Errorf("credential resource %q requires a resource store configured on the handler", config.Resource)
	}
	if config.Header != "" {
		binding.config.Header = http.CanonicalHeaderKey(strings.TrimSpace(config.Header))
		if p.staticHeader[binding.config.Header] != "" {
			return fmt.Errorf("credential header %q is also a static header", binding.config.Header)
		}
		for _, request := range p.request {
			if request.target == targetHeader && request.name == binding.config.Header {
				return fmt.Errorf("credential header %q is also a request mapping", binding.config.Header)
			}
		}
	}
	if config.Argument != "" {
		if p.transport != TransportMCP {
			return fmt.Errorf("credential argument requires transport %q", TransportMCP)
		}
		for _, request := range p.request {
			if request.target == targetArgument && request.name == config.Argument {
				return fmt.Errorf("credential argument %q is also a request mapping", config.Argument)
			}
		}
	}
	p.credential = binding
	return nil
}

func (p *plan) compileResponse(config Response) error {
	source := config.Source
	switch p.transport {
	case TransportHTTP:
		if source == "" {
			source = SourceJSON
		}
		if source != SourceJSON {
			return fmt.Errorf("response source %q is not supported for transport %q", source, TransportHTTP)
		}
	case TransportMCP:
		if source == "" {
			source = SourceStructuredContent
		}
		if source != SourceStructuredContent && source != SourceText {
			return fmt.Errorf("response source %q must be %q or %q for transport %q", source, SourceStructuredContent, SourceText, TransportMCP)
		}
	}
	p.source = source
	if len(config.Mappings) == 0 {
		return fmt.Errorf("response requires at least one mapping")
	}
	mappings := make([]transform.Mapping, 0, len(config.Mappings))
	for index, mapping := range config.Mappings {
		path, err := transform.CompilePointer(mapping.Path)
		if err != nil {
			return fmt.Errorf("response.mappings[%d]: %w", index, err)
		}
		output := strings.TrimSpace(mapping.Output)
		if output == "" {
			return fmt.Errorf("response.mappings[%d]: output path is required", index)
		}
		mappings = append(mappings, transform.Mapping{From: path, To: output, Required: !mapping.Optional})
	}
	var err error
	p.response, err = transform.Compile(p.outputType, mappings)
	if err != nil {
		paths := make([]string, len(mappings))
		for i, mapping := range mappings {
			paths[i] = mapping.To
		}
		return fmt.Errorf("response output paths %q: %w", paths, err)
	}
	return nil
}

func (p *plan) compileCache(config *Cache, validity *Validity) error {
	if config == nil {
		if validity != nil {
			return fmt.Errorf("response validity requires a cache section")
		}
		return nil
	}
	ttl, err := time.ParseDuration(config.TTL)
	if err != nil {
		return fmt.Errorf("cache ttl: %w", err)
	}
	if ttl <= 0 {
		return fmt.Errorf("cache ttl %s must be positive", config.TTL)
	}
	p.ttl = ttl
	if strings.TrimSpace(config.Name) == "" {
		return fmt.Errorf("cache name is required for explicit backend registration")
	}
	p.cacheName = config.Name
	seen := map[string]bool{}
	for index, path := range config.Partition {
		input, err := p.lookupInput(path)
		if err != nil {
			return fmt.Errorf("cache.partition[%d]: %w", index, err)
		}
		if seen[input.path] {
			return fmt.Errorf("cache.partition[%d]: %q is listed more than once", index, input.path)
		}
		seen[input.path] = true
		p.partition = append(p.partition, input)
	}
	if validity != nil {
		path, err := transform.CompilePointer(validity.Path)
		if err != nil {
			return fmt.Errorf("response.validity: %w", err)
		}
		switch validity.Format {
		case ValidityUnix, ValidityRFC3339, ValiditySeconds:
		default:
			return fmt.Errorf("response.validity format %q must be %q, %q or %q", validity.Format, ValidityUnix, ValidityRFC3339, ValiditySeconds)
		}
		p.validity = &validityBinding{path: path, format: validity.Format}
	}
	return nil
}

// materials is the fully mapped outbound request for one invocation.
type materials struct {
	header    http.Header
	query     url.Values
	path      map[string]string
	body      any
	hasBody   bool
	arguments map[string]any
	token     string
	partition []string
}

// outbound returns the explicit per-invocation header set and tool arguments:
// the mapped request values plus the resolved credential placement. Static
// configured headers are applied by the client, not here.
func (p *plan) outbound(m *materials) (http.Header, map[string]any) {
	header := m.header.Clone()
	if header == nil {
		header = http.Header{}
	}
	arguments := make(map[string]any, len(m.arguments)+1)
	for name, value := range m.arguments {
		arguments[name] = value
	}
	if p.credential != nil {
		if p.credential.config.Argument != "" {
			arguments[p.credential.config.Argument] = m.token
		} else {
			header.Set(p.credential.config.Header, m.token)
		}
	}
	return header, arguments
}

func (p *plan) materialize(input any, resources fs.FS) (*materials, error) {
	result := &materials{header: http.Header{}, query: url.Values{}, path: map[string]string{}, arguments: map[string]any{}}
	var body map[string]any
	for _, binding := range p.request {
		value, _, err := binding.input.plan.Select(input)
		if err != nil {
			return nil, fmt.Errorf("input %q: %w", binding.input.path, err)
		}
		value, present := dereference(value)
		switch binding.target {
		case targetHeader, targetQuery, targetPath:
			if !present {
				if binding.target == targetPath {
					return nil, fmt.Errorf("input %q mapped to url placeholder {%s} has no value", binding.input.path, binding.name)
				}
				continue
			}
			text, err := scalarText(value)
			if err != nil {
				return nil, fmt.Errorf("input %q mapped to %s %q: %w", binding.input.path, binding.target, binding.name, err)
			}
			switch binding.target {
			case targetHeader:
				result.header.Set(binding.name, text)
			case targetQuery:
				result.query.Add(binding.name, text)
			case targetPath:
				result.path[binding.name] = text
			}
		case targetArgument:
			if present {
				result.arguments[binding.name] = value
			}
		case targetBody:
			if !present {
				continue
			}
			if binding.body.String() == "" {
				result.body, result.hasBody = value, true
				continue
			}
			if body == nil {
				body = map[string]any{}
				result.body, result.hasBody = body, true
			}
			if err := binding.body.Assign(body, value); err != nil {
				return nil, err
			}
		}
	}
	if p.credential != nil {
		token, err := p.resolveCredential(resources)
		if err != nil {
			return nil, err
		}
		result.token = token
	}
	for _, partition := range p.partition {
		value, _, err := partition.plan.Select(input)
		if err != nil {
			return nil, fmt.Errorf("cache partition input %q: %w", partition.path, err)
		}
		value, present := dereference(value)
		if !present {
			return nil, fmt.Errorf("cache partition input %q has no value", partition.path)
		}
		text, err := scalarText(value)
		if err != nil {
			return nil, fmt.Errorf("cache partition input %q: %w", partition.path, err)
		}
		result.partition = append(result.partition, text)
	}
	return result, nil
}

// resolveCredential reads the server-owned secret verbatim; only surrounding
// whitespace is trimmed and no scheme is added or rewritten.
func (p *plan) resolveCredential(resources fs.FS) (string, error) {
	if resources == nil {
		return "", fmt.Errorf("credential resource %q requires a resource store", p.credential.config.Resource)
	}
	data, err := fs.ReadFile(resources, p.credential.config.Resource)
	if err != nil {
		return "", fmt.Errorf("read credential resource %q: %w", p.credential.config.Resource, err)
	}
	token := string(bytes.TrimSpace(data))
	if token == "" {
		return "", fmt.Errorf("credential resource %q is empty", p.credential.config.Resource)
	}
	return token, nil
}

func (p *plan) cacheKey(m *materials) string {
	type keyMaterial struct {
		Identity  string            `json:"identity"`
		Header    http.Header       `json:"header"`
		Query     url.Values        `json:"query"`
		Path      map[string]string `json:"path"`
		Body      any               `json:"body"`
		HasBody   bool              `json:"hasBody"`
		Arguments map[string]any    `json:"arguments"`
		Token     string            `json:"token"`
		Partition []string          `json:"partition"`
	}
	encoded, err := json.Marshal(keyMaterial{
		Identity: p.identity, Header: m.header, Query: m.query, Path: m.path, Body: m.body, HasBody: m.hasBody,
		Arguments: m.arguments, Token: m.token, Partition: m.partition,
	})
	if err != nil {
		// Unencodable request materials cannot be cached safely; a unique key
		// forces a fresh invocation without ever aliasing another entry.
		return p.identity + ":" + fmt.Sprintf("%p", m)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func partitionKey(values []string) string {
	return strings.Join(values, "\x00")
}

// populate delegates typed destination assignment and strict decoding to
// Bindly through the reusable transform plan.
func (p *plan) populate(ctx context.Context, document any, output any) error {
	return p.response.Apply(ctx, document, output)
}

// expiry reads the explicit validity limit from the document.
func (p *plan) expiry(document any, now time.Time) (time.Time, bool, error) {
	if p.validity == nil {
		return time.Time{}, false, nil
	}
	value, found, err := p.validity.path.Select(document)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("response validity: %w", err)
	}
	if !found || value == nil {
		return time.Time{}, false, fmt.Errorf("response validity path %s is missing", p.validity.path)
	}
	switch p.validity.format {
	case ValidityRFC3339:
		text, ok := value.(string)
		if !ok {
			return time.Time{}, false, fmt.Errorf("response validity requires an RFC 3339 string, got %T", value)
		}
		parsed, err := time.Parse(time.RFC3339, text)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("response validity: %w", err)
		}
		return parsed, true, nil
	default:
		number, ok := value.(json.Number)
		if !ok {
			return time.Time{}, false, fmt.Errorf("response validity requires a number, got %T", value)
		}
		seconds, err := number.Float64()
		if err != nil {
			return time.Time{}, false, fmt.Errorf("response validity: %w", err)
		}
		if p.validity.format == ValiditySeconds {
			return now.Add(time.Duration(seconds * float64(time.Second))), true, nil
		}
		return time.Unix(0, int64(seconds*float64(time.Second))), true, nil
	}
}

func dereference(value any) (any, bool) {
	if value == nil {
		return nil, false
	}
	actual := reflect.ValueOf(value)
	for actual.Kind() == reflect.Pointer || actual.Kind() == reflect.Interface {
		if actual.IsNil() {
			return nil, false
		}
		actual = actual.Elem()
	}
	return actual.Interface(), true
}

func scalarText(value any) (string, error) {
	actual := reflect.ValueOf(value)
	switch actual.Kind() {
	case reflect.String:
		return actual.String(), nil
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return fmt.Sprint(value), nil
	default:
		return "", fmt.Errorf("value of type %T is not a scalar", value)
	}
}
