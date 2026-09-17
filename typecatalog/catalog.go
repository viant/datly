package typecatalog

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	x "github.com/viant/x"
	xresponse "github.com/viant/xdatly/response"
)

// TypeOrigin identifies where a structural type entered Datly's authority.
type TypeOrigin string

const (
	TypeOriginPackage   TypeOrigin = "package"
	TypeOriginGenerated TypeOrigin = "generated"
	TypeOriginDQL       TypeOrigin = "dql"
)

// Authority selects the source-of-truth order for an immutable catalog view.
type Authority string

const (
	PackageAuthority    Authority = "package"
	TranscribeAuthority Authority = "transcribe"
)

type registration struct {
	Origin TypeOrigin
	Type   *x.Type
}

// Catalog retains all known structural types without assigning one global
// precedence. Callers select package or transcribe authority when resolving.
type Catalog struct {
	mu    sync.RWMutex
	items map[string][]registration
}

func NewCatalog() *Catalog {
	result := &Catalog{items: map[string][]registration{}}
	for _, typ := range standardTypes() {
		result.items[typ.Key()] = []registration{{Origin: TypeOriginPackage, Type: typ}}
	}
	return result
}

func standardTypes() []*x.Type {
	return []*x.Type{
		x.NewType(reflect.TypeOf(xresponse.Status{})),
	}
}

func (c *Catalog) Register(origin TypeOrigin, typ *x.Type) error {
	return c.RegisterAll(origin, typ)
}

// RegisterAll atomically adds a group of types under one origin. Validation or
// conflicts leave the catalog unchanged.
func (c *Catalog) RegisterAll(origin TypeOrigin, types ...*x.Type) error {
	if c == nil {
		return fmt.Errorf("type catalog is required")
	}
	if !validTypeOrigin(origin) {
		return fmt.Errorf("unknown type origin %q", origin)
	}
	detached := make(map[string]*x.Type, len(types))
	for _, typ := range types {
		if typ == nil {
			return fmt.Errorf("type is required")
		}
		copy, err := (x.Cloner{}).Type(typ)
		if err != nil {
			return err
		}
		key := strings.TrimSpace(copy.Key())
		if key == "" {
			return fmt.Errorf("type key is required")
		}
		if existing := detached[key]; existing != nil && !equalType(existing, copy) {
			return fmt.Errorf("type %q occurs more than once with different definitions", key)
		}
		detached[key] = copy
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, typ := range detached {
		for _, existing := range c.items[key] {
			if existing.Origin != origin {
				continue
			}
			if !equalType(existing.Type, typ) {
				return fmt.Errorf("type %q is already registered from origin %q", key, origin)
			}
		}
	}
	for key, typ := range detached {
		alreadyRegistered := false
		for _, existing := range c.items[key] {
			if existing.Origin == origin {
				alreadyRegistered = true
				break
			}
		}
		if !alreadyRegistered {
			c.items[key] = append(c.items[key], registration{Origin: origin, Type: typ})
		}
	}
	return nil
}

// Clone returns a detached catalog snapshot that can be safely extended
// without mutating the source catalog.
func (c *Catalog) Clone() (*Catalog, error) {
	if c == nil {
		return nil, fmt.Errorf("type catalog is required")
	}
	result := NewCatalog()
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Registrations own detached descriptors and never mutate them. Public
	// lookups clone descriptors; package updates replace registrations. Copy
	// the mutable index while sharing these private immutable values.
	for key, registrations := range c.items {
		result.items[key] = append([]registration(nil), registrations...)
	}
	return result, nil
}

func (c *Catalog) Resolve(authority Authority, key string) (*x.Type, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("type catalog is required")
	}
	if err := validateAuthority(authority); err != nil {
		return nil, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	registrations := c.items[strings.TrimSpace(key)]
	typ, ok := resolveRegistrations(authority, registrations)
	detached, err := (x.Cloner{}).Type(typ)
	return detached, ok, err
}

// ResolveRuntimeType returns only the immutable Go identity of the selected
// descriptor. A found synthetic-only declaration returns nil, true; it must not
// fall through to a lower-priority origin's compiled type.
func (c *Catalog) ResolveRuntimeType(authority Authority, key string) (reflect.Type, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("type catalog is required")
	}
	if err := validateAuthority(authority); err != nil {
		return nil, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	typ, ok := resolveRegistrations(authority, c.items[strings.TrimSpace(key)])
	if !ok {
		return nil, false, nil
	}
	return typ.Type, true, nil
}

func resolveRegistrations(authority Authority, registrations []registration) (*x.Type, bool) {
	if len(registrations) == 0 {
		return nil, false
	}
	best := registrations[0]
	bestPriority := originPriority(authority, best.Origin)
	for _, candidate := range registrations[1:] {
		priority := originPriority(authority, candidate.Origin)
		if priority > bestPriority {
			best = candidate
			bestPriority = priority
		}
	}
	return best.Type, true
}

// Registry returns a deterministic snapshot for one authority. Later catalog
// registrations cannot mutate the returned registry.
func (c *Catalog) Registry(authority Authority) (*x.Registry, error) {
	if c == nil {
		return nil, fmt.Errorf("type catalog is required")
	}
	if err := validateAuthority(authority); err != nil {
		return nil, err
	}
	registry := x.NewRegistry()
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if typ, ok := resolveRegistrations(authority, c.items[key]); ok {
			detached, err := (x.Cloner{}).Type(typ)
			if err != nil {
				return nil, err
			}
			registry.Register(detached)
		}
	}
	return registry, nil
}

// resolverTypes selects private, immutable registrations for a resolver snapshot.
// Callers must detach descriptors before passing them out of the catalog owner.
func (c *Catalog) resolverTypes(authority Authority) (map[string]*x.Type, error) {
	if c == nil {
		return nil, fmt.Errorf("type catalog is required")
	}
	if err := validateAuthority(authority); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*x.Type, len(c.items))
	for key, registrations := range c.items {
		if typ, ok := resolveRegistrations(authority, registrations); ok {
			result[key] = typ
		}
	}
	return result, nil
}

func equalType(left, right *x.Type) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Key() == right.Key() &&
		left.Type == right.Type &&
		left.Location == right.Location &&
		left.Definition == right.Definition &&
		left.Scn == right.Scn &&
		left.Force == right.Force &&
		equalSyntheticType(left.SynteticType, right.SynteticType)
}

func validTypeOrigin(origin TypeOrigin) bool {
	switch origin {
	case TypeOriginPackage, TypeOriginGenerated, TypeOriginDQL:
		return true
	default:
		return false
	}
}

func validateAuthority(authority Authority) error {
	if authority != PackageAuthority && authority != TranscribeAuthority {
		return fmt.Errorf("unknown type authority %q", authority)
	}
	return nil
}

func originPriority(authority Authority, origin TypeOrigin) int {
	if authority == PackageAuthority {
		switch origin {
		case TypeOriginPackage:
			return 30
		case TypeOriginGenerated:
			return 20
		case TypeOriginDQL:
			return 10
		}
	}
	switch origin {
	case TypeOriginDQL:
		return 30
	case TypeOriginPackage:
		return 20
	case TypeOriginGenerated:
		return 10
	default:
		return 0
	}
}
