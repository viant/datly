// Package contract resolves caller-dependent metadata without executing the
// component's business handler. The caller supplies its native binding scope.
package contract

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/viant/bindly"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

// Plan belongs to one immutable component registration and one direction. Do not
// share it across routes/configurations with different policy authority. Only
// effective types are retained; request binders, receivers and closures are not.
type Plan struct {
	original    reflect.Type
	providers   []contractOwner
	markerPaths []string
	walk        *shape.RewritePlan
	capacity    int
	mu          sync.Mutex
	variants    map[string]*list.Element
	order       list.List
}

type contractOwner struct {
	path string
	typ  reflect.Type
}

type fieldKey struct{ path, name string }
type variant struct {
	edits          map[fieldKey]*shape.FieldMetadata
	markers        map[fieldKey]string
	name           string
	representation *shape.Representation
	once           sync.Once
	typ            reflect.Type
	err            error
}

// New detects the hook on the original linked type. capacity bounds retained
// named variants; zero disables retention. Hook discovery includes nested owners.
// A graph without hooks retains no walker after discovery.
func New(original reflect.Type, capacity int) (*Plan, error) {
	if original == nil {
		return nil, fmt.Errorf("contract type is required")
	}
	if capacity < 0 {
		return nil, fmt.Errorf("contract cache capacity must not be negative")
	}
	p := &Plan{original: original, capacity: capacity}
	var err error
	p.walk, err = shape.CompileRewrite(original)
	if err != nil {
		return nil, err
	}
	provider := reflect.TypeFor[xcontract.Provider]()
	err = p.walk.VisitStructs(func(path string, typ reflect.Type) error {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Tag.Get("setMarker") == "true" {
				markerPath := field.Name
				if path != "" {
					markerPath = path + "." + markerPath
				}
				p.markerPaths = append(p.markerPaths, markerPath)
			}
		}
		if reflect.PointerTo(typ).Implements(provider) {
			p.providers = append(p.providers, contractOwner{path: path, typ: typ})
		}
		return nil
	})
	if len(p.providers) == 0 {
		p.walk = nil
	}
	return p, err
}

func (p *Plan) Original() reflect.Type { return p.original }
func (p *Plan) Dynamic() bool          { return len(p.providers) > 0 }

// Resolve evaluates current caller policy even on cache hits. Nil policies and
// unchanged traversals return exact linked types. A fresh receiver prevents one
// invocation's receiver fields from leaking into another invocation.
func (p *Plan) Resolve(ctx context.Context, binder bindly.Binder) (reflect.Type, error) {
	selected, err := p.resolve(ctx, binder)
	if err != nil {
		return nil, err
	}
	if selected == nil {
		return p.original, nil
	}
	selected.once.Do(func() {
		if selected.representation == nil {
			selected.typ = p.original
			return
		}
		metadata := selected.representation
		if len(selected.markers) > 0 {
			metadata, selected.err = p.walk.RewriteRepresentation(func(path string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
				key := fieldKey{path, field.Name}
				edit := selected.edits[key]
				if name, ok := selected.markers[key]; ok {
					if edit == nil {
						copy := *field
						edit = &copy
					} else {
						copy := *edit
						edit = &copy
					}
					edit.Name = name
				}
				return edit, nil
			})
			if selected.err != nil {
				return
			}
		}

		selected.typ, selected.err = metadata.Reflected()
	})
	return selected.typ, selected.err
}

// ResolveRepresentation returns opt-in metadata only when a policy changes the
// contract. It never synthesizes storage types and therefore supports linked
// method-bearing embeddings. Nil means use the original linked codec path.
func (p *Plan) ResolveRepresentation(ctx context.Context, binder bindly.Binder) (*shape.Representation, error) {
	selected, err := p.resolve(ctx, binder)
	if err != nil || selected == nil {
		return nil, err
	}
	return selected.representation, nil
}

func (p *Plan) resolve(ctx context.Context, binder bindly.Binder) (*variant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !p.Dynamic() {
		return nil, nil
	}
	if binder == nil {
		return nil, fmt.Errorf("dynamic contract binder is required")
	}
	type resolvedOwner struct {
		skipped []string
		owner   contractOwner
		policy  *xcontract.Policy
	}
	var policies []resolvedOwner
	type keyPart struct {
		Path, Name string
		Active     bool
	}
	parts := make([]keyPart, 0, len(p.providers))
	cacheable := true
	for _, owner := range p.providers {
		policy, err := reflect.New(owner.typ).Interface().(xcontract.Provider).Contract(ctx, binder)
		if err != nil {
			return nil, fmt.Errorf("resolve contract policy at %s (%s): %w", owner.path, owner.typ, err)
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		part := keyPart{Path: owner.path}
		if policy != nil && policy.NoCache {
			cacheable = false
		}
		if policy != nil && policy.Field != nil {
			policies = append(policies, resolvedOwner{owner: owner, policy: policy})
			part.Name, part.Active = policy.Name, true
			if policy.Name == "" {
				cacheable = false
			}
		}
		parts = append(parts, part)
	}
	if len(policies) == 0 {
		return nil, nil
	}
	name := ""
	if cacheable {
		encoded, err := json.Marshal(parts)
		if err != nil {
			return nil, err
		}
		name = string(encoded)
	}
	var err error
	// Serialize named misses; successful callers share only the immutable type.
	// Unnamed policies never acquire/cache a cross-request entry.
	if name != "" && p.capacity > 0 {
		p.mu.Lock()
		defer p.mu.Unlock()
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if item := p.variants[name]; item != nil {
			p.order.MoveToFront(item)
			return item.Value.(*variant), nil
		}
	}
	edits := map[fieldKey]*shape.FieldMetadata{}
	markers := map[fieldKey]string{}
	effective, err := p.walk.RewriteRepresentation(func(path string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Presence storage is infrastructure metadata, not a business field.
		// Retain it unchanged here; the recorded rename pass below synchronizes
		// marker names only when a reflected representation needs that mapping.
		if field.Tag.Get("setMarker") == "true" {
			return nil, nil
		}
		for _, marker := range p.markerPaths {
			if path == marker || strings.HasPrefix(path, marker+".") || strings.HasPrefix(path, marker+"[]") || strings.HasPrefix(path, marker+"{}") {
				return nil, nil
			}
		}
		branch := field.Name
		if path != "" {
			branch = path + "." + branch
		}
		current := &xcontract.Field{Name: field.Name, Tag: field.Tag}
		// Nested owners apply first. Enclosing policies inspect the accumulated
		// result and have final authority; nil means retain the nested decision.
		for i := len(policies) - 1; i >= 0; i-- {
			selected := &policies[i]
			ownerPath := selected.owner.path
			if !withinContractPath(path, ownerPath) || skippedContractPath(path, selected.skipped) {
				continue
			}
			candidate, err := selected.policy.Field(path, parent, current.Clone())
			if err != nil {
				return nil, err
			}
			if candidate == nil {
				continue
			}
			if err = candidate.Err(); err != nil {
				return nil, err
			}
			if candidate.ChildrenSkipped() {
				selected.skipped = append(selected.skipped, branch)
			}
			// Traversal control belongs to this policy, never to the next
			// enclosing policy inspecting the accumulated Name/Tag metadata.
			current = &xcontract.Field{Name: candidate.Name, Tag: candidate.Tag}
		}
		descend := false
		for i := range policies {
			selected := &policies[i]
			if skippedContractPath(branch, selected.skipped) {
				continue
			}
			if withinContractPath(branch, selected.owner.path) || withinContractPath(selected.owner.path, branch) {
				descend = true
				break
			}
		}
		candidate := current
		if candidate.Name == field.Name && candidate.Tag == field.Tag {
			if !descend {
				return &shape.FieldMetadata{Name: field.Name, Tag: field.Tag, SkipChildren: true}, nil
			}
			return nil, nil
		}
		original, ok := parent.FieldByName(field.Name)
		if !ok {
			return nil, fmt.Errorf("original contract field %s is missing", field.Name)
		}
		if err = candidate.Validate(original.Type); err != nil {
			return nil, err
		}
		edit := &shape.FieldMetadata{Name: candidate.Name, Tag: candidate.Tag, SkipChildren: !descend}
		edits[fieldKey{path, field.Name}] = edit
		if candidate.Name != field.Name {
			// Native decoders address set-marker flags by Go field name. Rename
			// the corresponding effective flag too; value projection maps it back
			// to the original bool position before the business handler sees it.
			for i := 0; i < parent.NumField(); i++ {
				holder := parent.Field(i)
				if holder.Tag.Get("setMarker") != "true" {
					continue
				}
				typ := holder.Type
				for typ.Kind() == reflect.Pointer {
					typ = typ.Elem()
				}
				if typ.Kind() != reflect.Struct {
					continue
				}
				flag, ok := typ.FieldByName(field.Name)
				if !ok || flag.Type.Kind() != reflect.Bool {
					continue
				}
				markerPath := holder.Name
				if path != "" {
					markerPath = path + "." + holder.Name
				}
				markers[fieldKey{markerPath, field.Name}] = candidate.Name
			}
		}
		return edit, nil
	})
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	selected := &variant{name: name, representation: effective}
	if len(markers) > 0 {
		selected.edits = edits
		selected.markers = markers
	}
	if !effective.Changed() {
		selected.representation = nil
	}
	if name != "" && p.capacity > 0 {
		if p.variants == nil {
			p.variants = make(map[string]*list.Element)
		}
		p.variants[name] = p.order.PushFront(selected)
		if p.order.Len() > p.capacity {
			last := p.order.Back()
			delete(p.variants, last.Value.(*variant).name)
			p.order.Remove(last)
		}
	}
	return selected, nil
}

func withinContractPath(path, owner string) bool {
	return owner == "" || path == owner || strings.HasPrefix(path, owner+".") || strings.HasPrefix(path, owner+"[]") || strings.HasPrefix(path, owner+"{}")
}
func skippedContractPath(path string, skipped []string) bool {
	for _, owner := range skipped {
		if withinContractPath(path, owner) {
			return true
		}
	}
	return false
}
