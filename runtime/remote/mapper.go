package remote

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	coordinator "github.com/viant/datly/runtime/remote/cache"
	xcache "github.com/viant/xdatly/cache"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

// Mapper is the shared request/response mapping and result-cache helper an
// ordinary custom handler calls from Exec. It receives the configuration, the
// selected provider and the typed input/output explicitly; it discovers no
// fields, binds no dependencies and owns no clients. One Mapper may serve any
// number of handlers; compiled plans and cache entries are scoped by
// configuration identity and by the concrete input/output types.
type Mapper struct {
	clock     func() time.Time
	resources fs.FS
	plans     sync.Map
	cache     sync.Map // cache name -> *coordinator.Coordinator (metadata only)
}

// Option configures a Mapper.
type Option func(*Mapper)

// WithClock replaces the cache clock; tests use it for deterministic expiry.
func WithClock(clock func() time.Time) Option {
	return func(m *Mapper) {
		if clock != nil {
			m.clock = clock
		}
	}
}

// WithResources supplies the server-owned store that resolves
// credential.resource references. Without it, credential declarations fail.
func WithResources(resources fs.FS) Option {
	return func(m *Mapper) { m.resources = resources }
}

// NewMapper builds a helper with no backend. Enabled declarations resolve a
// named cache from the ordinary handler's bound xcache.Provider.
func NewMapper(options ...Option) *Mapper {
	result := &Mapper{clock: time.Now}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	return result
}

// HTTP maps input onto the configured HTTP request, executes it through the
// client the provider resolves for config.Client.HTTP, and maps the JSON
// response into output. input and output must be non-nil struct pointers;
// mapping paths are resolved against their types. config must select
// transport "http".
func (m *Mapper) HTTP(ctx context.Context, config *Config, provider xhttp.Provider, cacheProvider xcache.Provider, input, output any) error {
	compiled, err := m.prepare(config, TransportHTTP, input, output)
	if err != nil {
		return err
	}
	if nilCapability(provider) {
		return fmt.Errorf("remote http provider is not bound")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := compiled.materialize(input, m.resources)
	if err != nil {
		return err
	}
	backend, err := resolveCache(ctx, compiled, cacheProvider)
	if err != nil {
		return err
	}
	return m.run(ctx, compiled, request, output, backend, func(ctx context.Context, request *materials) (any, error) {
		client, err := provider.Client(ctx, *config.Client.HTTP)
		if err != nil {
			return nil, fmt.Errorf("remote http client: %w", err)
		}
		if nilCapability(client) {
			return nil, fmt.Errorf("remote http provider returned no client")
		}
		return invokeHTTP(ctx, client, compiled, request)
	})
}

// MCP maps input onto the configured tool call, executes it through the
// client the provider resolves for config.Client.MCP, and maps the selected
// result document into output. config must select transport "mcp".
func (m *Mapper) MCP(ctx context.Context, config *Config, provider xmcp.Provider, cacheProvider xcache.Provider, input, output any) error {
	compiled, err := m.prepare(config, TransportMCP, input, output)
	if err != nil {
		return err
	}
	if nilCapability(provider) {
		return fmt.Errorf("remote mcp provider is not bound")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := compiled.materialize(input, m.resources)
	if err != nil {
		return err
	}
	backend, err := resolveCache(ctx, compiled, cacheProvider)
	if err != nil {
		return err
	}
	return m.run(ctx, compiled, request, output, backend, func(ctx context.Context, request *materials) (any, error) {
		client, err := provider.Client(ctx, *config.Client.MCP)
		if err != nil {
			return nil, fmt.Errorf("remote mcp client: %w", err)
		}
		if nilCapability(client) {
			return nil, fmt.Errorf("remote mcp provider returned no client")
		}
		return invokeMCP(ctx, client, compiled, request)
	})
}

func resolveCache(ctx context.Context, compiled *plan, provider xcache.Provider) (xcache.Cache, error) {
	if compiled.ttl <= 0 {
		return nil, nil
	}
	if nilCapability(provider) {
		return nil, fmt.Errorf("remote cache provider is not bound for %q", compiled.cacheName)
	}
	backend, err := provider.Cache(ctx, compiled.cacheName)
	if err != nil {
		return nil, fmt.Errorf("remote cache %q: %w", compiled.cacheName, err)
	}
	if nilCapability(backend) {
		return nil, fmt.Errorf("remote cache %q returned no backend", compiled.cacheName)
	}
	return backend, nil
}

func (m *Mapper) coordinator(name string) *coordinator.Coordinator {
	if cached, ok := m.cache.Load(name); ok {
		return cached.(*coordinator.Coordinator)
	}
	actual, _ := m.cache.LoadOrStore(name, coordinator.New(m.clock))
	return actual.(*coordinator.Coordinator)
}

// Invalidate fences in-flight results and deletes one partition in a named
// backend. No partition values clears all entries tracked by this Mapper.
func (m *Mapper) Invalidate(ctx context.Context, config *Config, provider xcache.Provider, partition ...string) (int, error) {
	if config == nil || config.Cache == nil || config.Cache.Name == "" {
		return 0, fmt.Errorf("cache configuration with a name is required")
	}
	backend, err := resolveCache(ctx, &plan{ttl: time.Nanosecond, cacheName: config.Cache.Name}, provider)
	if err != nil {
		return 0, err
	}
	return m.coordinator(config.Cache.Name).Invalidate(ctx, backend, partitionKey(partition), len(partition) == 0)
}

// CachedEntries reconciles locally tracked keys against the named backend.
func (m *Mapper) CachedEntries(ctx context.Context, config *Config, provider xcache.Provider) (int, error) {
	if config == nil || config.Cache == nil || config.Cache.Name == "" {
		return 0, fmt.Errorf("cache configuration with a name is required")
	}
	backend, err := resolveCache(ctx, &plan{ttl: time.Nanosecond, cacheName: config.Cache.Name}, provider)
	if err != nil {
		return 0, err
	}
	return m.coordinator(config.Cache.Name).Count(ctx, backend)
}

type planKey struct {
	identity string
	input    reflect.Type
	output   reflect.Type
}

// prepare validates the configuration against the concrete types and returns
// the compiled plan before any provider or network interaction.
func (m *Mapper) prepare(config *Config, transport string, input, output any) (*plan, error) {
	if m == nil {
		return nil, fmt.Errorf("remote mapper is required; use remote.NewMapper")
	}
	if config == nil {
		return nil, fmt.Errorf("remote configuration is required")
	}
	inputType, err := structPointer(input, "input")
	if err != nil {
		return nil, err
	}
	outputType, err := structPointer(output, "output")
	if err != nil {
		return nil, err
	}
	identity, err := configIdentity(config)
	if err != nil {
		return nil, err
	}
	key := planKey{identity: identity, input: inputType, output: outputType}
	if cached, ok := m.plans.Load(key); ok {
		return m.checkTransport(cached.(*plan), transport)
	}
	compiled, err := compile(config, compileInput{inputType: inputType, outputType: outputType, hasResources: m.resources != nil})
	if err != nil {
		return nil, fmt.Errorf("remote configuration: %w", err)
	}
	actual, _ := m.plans.LoadOrStore(key, compiled)
	return m.checkTransport(actual.(*plan), transport)
}

func (m *Mapper) checkTransport(compiled *plan, transport string) (*plan, error) {
	if compiled.transport != transport {
		return nil, fmt.Errorf("remote configuration selects transport %q; call Mapper.%s with a matching provider", compiled.transport, methodFor(compiled.transport))
	}
	return compiled, nil
}

func methodFor(transport string) string {
	if transport == TransportMCP {
		return "MCP"
	}
	return "HTTP"
}

func structPointer(value any, label string) (reflect.Type, error) {
	if value == nil {
		return nil, fmt.Errorf("remote %s must be a non-nil struct pointer", label)
	}
	actual := reflect.TypeOf(value)
	if actual.Kind() != reflect.Pointer || actual.Elem().Kind() != reflect.Struct || reflect.ValueOf(value).IsNil() {
		return nil, fmt.Errorf("remote %s must be a non-nil struct pointer, got %T", label, value)
	}
	return actual.Elem(), nil
}

type invokeFunc func(ctx context.Context, request *materials) (any, error)

// run materializes the request, invokes (or serves the cache) and populates
// the caller's output.
func (m *Mapper) run(ctx context.Context, compiled *plan, request *materials, output any, backend xcache.Cache, invoke invokeFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	document, err := m.document(ctx, compiled, request, backend, invoke)
	if err != nil {
		return err
	}
	return compiled.populate(ctx, document, output)
}

func (m *Mapper) document(ctx context.Context, compiled *plan, request *materials, backend xcache.Cache, invoke invokeFunc) (any, error) {
	if compiled.ttl <= 0 {
		return invoke(ctx, request)
	}
	identity := compiled.cacheKey(request) + ":" + typeIdentity(compiled.inputType) + ":" + typeIdentity(compiled.outputType)
	sum := sha256.Sum256([]byte(identity))
	key := hex.EncodeToString(sum[:])
	partition := partitionKey(request.partition)
	coord := m.coordinator(compiled.cacheName)
	valid := func(data []byte) (time.Time, bool) {
		var entry cachedDocument
		if json.Unmarshal(data, &entry) != nil || !entry.Expires.After(m.clock()) || len(entry.Document) == 0 {
			return time.Time{}, false
		}
		return entry.Expires, true
	}
	data, _, err := coord.Resolve(ctx, backend, key, partition, valid, func(ctx context.Context) (coordinator.Outcome, error) {
		document, err := invoke(ctx, request)
		if err != nil {
			return coordinator.Outcome{}, err
		}
		// A successful transport response can still violate the output contract.
		// Validate before caching so a malformed response does not poison a
		// principal's cache entry for the configured TTL.
		if err := compiled.populate(ctx, document, reflect.New(compiled.outputType).Interface()); err != nil {
			return coordinator.Outcome{}, err
		}
		ttl := compiled.ttl
		now := m.clock()
		if expires, ok, err := compiled.expiry(document, now); err != nil {
			return coordinator.Outcome{}, err
		} else if ok && expires.Sub(now) < ttl {
			ttl = expires.Sub(now)
		}
		entry := cachedDocument{Expires: now.Add(ttl)}
		entry.Document, err = json.Marshal(document)
		if err != nil {
			return coordinator.Outcome{}, err
		}
		encoded, err := json.Marshal(entry)
		return coordinator.Outcome{Value: encoded, Expires: entry.Expires}, err
	})
	if err != nil {
		return nil, err
	}
	var entry cachedDocument
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("decode cached remote document: %w", err)
	}
	return decodeJSONDocument(entry.Document)
}

type cachedDocument struct {
	Expires  time.Time       `json:"expires"`
	Document json.RawMessage `json:"document"`
}

func typeIdentity(value reflect.Type) string {
	if value == nil {
		return ""
	}
	if value.Name() != "" {
		return value.PkgPath() + "." + value.Name()
	}
	switch value.Kind() {
	case reflect.Pointer:
		return "*" + typeIdentity(value.Elem())
	case reflect.Slice:
		return "[]" + typeIdentity(value.Elem())
	case reflect.Array:
		return "[" + strconv.Itoa(value.Len()) + "]" + typeIdentity(value.Elem())
	case reflect.Map:
		return "map[" + typeIdentity(value.Key()) + "]" + typeIdentity(value.Elem())
	case reflect.Struct:
		var result strings.Builder
		result.WriteString("struct{")
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			result.WriteString(strconv.Quote(field.PkgPath + "." + field.Name))
			result.WriteByte(':')
			result.WriteString(typeIdentity(field.Type))
			result.WriteByte(':')
			result.WriteString(strconv.Quote(string(field.Tag)))
			result.WriteByte(';')
		}
		result.WriteByte('}')
		return result.String()
	default:
		return value.String()
	}
}

func nilCapability(value any) bool {
	if value == nil {
		return true
	}
	r := reflect.ValueOf(value)
	switch r.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return r.IsNil()
	}
	return false
}
