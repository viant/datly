// Package index owns the immutable bootstrap map from protocol identities to
// authored component sources. Building the map does not load Go types, compile
// execution plans, open connectors, or create a paths.yaml file.
package index

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
)

type SourceKind string

const (
	SourceGo       SourceKind = "go"
	SourceDQL      SourceKind = "dql"
	SourceResource SourceKind = "resource"
)

// Source identifies one selected file and its bootstrap-time content digest.
type Source struct {
	Kind        SourceKind
	Path        string
	Dir         string
	PackagePath string
	Fingerprint string
}

// Entry is the immutable routing metadata and source authority for one
// component. Contract fields and executable plans are intentionally absent.
type Entry struct {
	Component *spec.Component
	// Warmup indicates that the component has root or child warmup declarations
	// discoverable during indexed bootstrap, before executable readers exist.
	Warmup bool
	// Owner identifies the authored component that materializes this entry when
	// the entry is a derived report component.
	Owner       spec.Key
	Sources     []Source
	Fingerprint string
}

func (e *Entry) Key() spec.Key {
	if e == nil || e.Component == nil {
		return spec.Key{}
	}
	return e.Component.Key
}

func (e *Entry) Clone() *Entry {
	if e == nil {
		return nil
	}
	return &Entry{Component: e.Component.Clone(), Warmup: e.Warmup, Owner: e.Owner, Sources: append([]Source(nil), e.Sources...), Fingerprint: e.Fingerprint}
}

type MCPIdentity struct {
	Kind spec.MCPExposureKind
	Name string
}

func (i MCPIdentity) key() string { return string(i.Kind) + "\x00" + strings.TrimSpace(i.Name) }

// Snapshot is a deterministic, immutable bootstrap index. Public accessors
// detach metadata so callers cannot mutate the published generation.
type Snapshot struct {
	Fingerprint       string
	SelectionIdentity string
	entries           []*Entry
	byComponent       map[string]*Entry
	byMCP             map[string]*Entry
	routes            *route.Bundle
}

func newSnapshot(selection, fingerprint string, entries []*Entry) (*Snapshot, error) {
	components := make([]*spec.Component, 0, len(entries))
	result := &Snapshot{Fingerprint: fingerprint, SelectionIdentity: selection, entries: entries, byComponent: map[string]*Entry{}, byMCP: map[string]*Entry{}}
	for _, entry := range entries {
		if entry == nil || entry.Component == nil {
			return nil, fmt.Errorf("bootstrap index component metadata is required")
		}
		identity := entry.Component.Key.String()
		if result.byComponent[identity] != nil {
			return nil, fmt.Errorf("duplicate bootstrap component %s", identity)
		}
		result.byComponent[identity] = entry
		components = append(components, entry.Component)
		for _, endpoint := range entry.Component.Routes {
			if endpoint == nil {
				continue
			}
			for _, exposure := range endpoint.MCP {
				if exposure == nil {
					return nil, fmt.Errorf("component %s route %s %s has a nil MCP exposure", identity, endpoint.Method, endpoint.Path)
				}
				mcpIdentity := MCPIdentity{Kind: exposure.Kind, Name: exposure.Identity(entry.Component, endpoint)}
				key := mcpIdentity.key()
				if mcpIdentity.Name == "" {
					return nil, fmt.Errorf("component %s has an empty MCP identity", identity)
				}
				if result.byMCP[key] != nil {
					return nil, fmt.Errorf("duplicate MCP %s name %q", exposure.Kind, mcpIdentity.Name)
				}
				result.byMCP[key] = entry
			}
		}
	}
	bundle, err := route.NewBundle(components)
	if err != nil {
		return nil, err
	}
	result.routes = bundle
	return result, nil
}

func (s *Snapshot) Entries() []*Entry {
	if s == nil {
		return nil
	}
	result := make([]*Entry, len(s.entries))
	for index := range s.entries {
		result[index] = s.entries[index].Clone()
	}
	return result
}

// Transform returns a new snapshot with detached component metadata changed by
// apply. Source fingerprints and selection identity remain authoritative.
func (s *Snapshot) Transform(apply func(*spec.Component)) (*Snapshot, error) {
	if s == nil {
		return nil, fmt.Errorf("bootstrap snapshot is required")
	}
	entries := s.Entries()
	for _, entry := range entries {
		if apply != nil {
			apply(entry.Component)
		}
	}
	return newSnapshot(s.SelectionIdentity, digestStrings(s.Fingerprint, "metadata"), entries)
}

func (s *Snapshot) Component(key spec.Key) (*Entry, bool) {
	if s == nil {
		return nil, false
	}
	entry, ok := s.byComponent[key.String()]
	return entry.Clone(), ok
}

func (s *Snapshot) Route(method, path string) (*Entry, *spec.Route, map[string]string, bool) {
	entry, endpoint, params, ok := s.route(method, path)
	if !ok {
		return nil, nil, nil, false
	}
	return entry.Clone(), endpoint.Clone(), params, true
}

func (s *Snapshot) route(method, path string) (*Entry, *spec.Route, map[string]string, bool) {
	if s == nil || s.routes == nil {
		return nil, nil, nil, false
	}
	component, params, ok := s.routes.ComponentByRouteWithParams(method, path)
	if !ok || component == nil {
		return nil, nil, nil, false
	}
	endpoint, ok := s.routes.RouteByMethodPath(method, path)
	if !ok {
		return nil, nil, nil, false
	}
	entry := s.byComponent[component.Key.String()]
	return entry, endpoint, params, entry != nil
}

func (s *Snapshot) MCP(identity MCPIdentity) (*Entry, bool) {
	if s == nil {
		return nil, false
	}
	entry, ok := s.byMCP[identity.key()]
	return entry.Clone(), ok
}

func (s *Snapshot) component(key spec.Key) (*Entry, bool) {
	if s == nil {
		return nil, false
	}
	entry, ok := s.byComponent[key.String()]
	return entry, ok
}

func sortEntries(entries []*Entry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Component.Key.String() < entries[j].Component.Key.String() })
}
