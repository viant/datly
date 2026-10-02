package index

import (
	"fmt"
	"sort"

	"github.com/viant/datly/spec"
)

// BuildLinked publishes routing metadata from compiled Go contracts. It does
// not inspect a workspace or track source files. A new binary supplies a new
// generation of linked contracts on restart.
func BuildLinked(packages []string, components []*spec.Component) (*Snapshot, error) {
	if len(components) == 0 {
		return nil, fmt.Errorf("no linked reflected components found")
	}
	entries := make([]*Entry, 0, len(components))
	for _, component := range components {
		if component == nil {
			return nil, fmt.Errorf("linked component metadata is required")
		}
		entries = append(entries, &Entry{
			Component: component.Clone(),
			Warmup:    hasComponentWarmupConfiguration(component) || hasDelegatedWarmupTarget(component),
		})
	}
	entries = expandReportEntries(entries)
	sortEntries(entries)
	selected := append([]string(nil), packages...)
	sort.Strings(selected)
	selection := digestStrings(append([]string{"datly-linked-bootstrap-v1"}, selected...)...)
	return newSnapshot(selection, snapshotFingerprint(selection, entries), entries)
}
