package index

import (
	"fmt"
	"sort"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
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
	entries = expandReportEntries(entries)
	sortEntries(entries)
	selected := append([]string(nil), packages...)
	sort.Strings(selected)
	selection := digestStrings(append([]string{"datly-linked-bootstrap-v1"}, selected...)...)
	return newSnapshot(selection, snapshotFingerprint(selection, entries), entries)
}
