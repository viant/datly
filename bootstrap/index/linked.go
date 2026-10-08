package index

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// BuildLinked publishes routing metadata from compiled Go contracts. It does
// not inspect a workspace or track source files. A new binary supplies a new
// generation of linked contracts on restart. Already reflected routes retain
// nested output warmup declarations not yet materialized in the component graph.
func BuildLinked(packages []string, components []*spec.Component, routes ...*bootstrap.RouteSource) (*Snapshot, error) {
	if len(components) == 0 {
		return nil, fmt.Errorf("no linked reflected components found")
	}
	linkedWarmups, err := linkedWarmupComponents(routes)
	if err != nil {
		return nil, err
	}
	entries := make([]*Entry, 0, len(components))
	for _, component := range components {
		if component == nil {
			return nil, fmt.Errorf("linked component metadata is required")
		}
		entries = append(entries, &Entry{
			Component: component.Clone(),
			Warmup:    linkedWarmups[component.Key.String()] || hasComponentWarmupConfiguration(component) || hasDelegatedWarmupTarget(component),
		})
	}
	if err := validateLinkedCubeEntries(entries); err != nil {
		return nil, err
	}
	entries = expandReportEntries(entries)
	sortEntries(entries)
	selected := append([]string(nil), packages...)
	sort.Strings(selected)
	selection := digestStrings(append([]string{"datly-linked-bootstrap-v1"}, selected...)...)
	return newSnapshot(selection, snapshotFingerprint(selection, entries), entries)
}

func validateLinkedCubeEntries(entries []*Entry) error {
	byKey := map[string]*spec.Component{}
	for _, entry := range entries {
		if entry != nil && entry.Component != nil {
			byKey[entry.Key().String()] = entry.Component
		}
	}
	for _, entry := range entries {
		source := entry.Component
		if source.Settings == nil || source.Settings.Report == nil || !source.Settings.Report.LinkedFacade {
			continue
		}
		var routes []*spec.Route
		for _, route := range source.Routes {
			if route != nil && strings.EqualFold(route.Method, http.MethodGet) {
				routes = append(routes, route)
			}
		}
		for _, route := range routes {
			suffix := ""
			if len(routes) > 1 {
				suffix = typecatalog.ExportedFieldName(route.Name)
			}
			key := spec.Key{Kind: spec.KindComponent, Scope: source.Key.Scope, Name: typecatalog.ExportedFieldName(source.Key.Name + suffix + "Cube")}
			cube := byKey[key.String()]
			if cube == nil {
				return fmt.Errorf("source %s requires linked cube %s; regenerate and link its facade", source.Key.String(), key.String())
			}
			found := false
			for _, candidate := range cube.Routes {
				if candidate != nil && strings.EqualFold(candidate.Method, http.MethodPost) && candidate.Path == strings.TrimRight(route.Path, "/")+"/cube" {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("linked cube %s does not expose its source cube route", key.String())
			}
		}
	}
	return nil
}
