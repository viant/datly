package sql

import (
	"fmt"
	"sync"

	"github.com/viant/datly/internal/sqlanalysis"
)

// ProjectionMembership retains checked, source-invariant output membership.
// It is immutable and contains no executable SQL or request values.
type ProjectionMembership struct {
	columns    []ProjectionColumn
	wildcard   bool
	index      map[string]int
	sourceOnly map[string]bool
}

// PrepareMembership reuses the native checked SQL analysis and output mappings.
// Failure means membership needs the evaluated builder source; it does not make
// an otherwise supported dynamic reader invalid.
func (p SelectorProjection) PrepareMembership() (*ProjectionMembership, error) {
	source, parsed, err := sqlanalysis.Checked(p.SQL, nil)
	if err != nil {
		return nil, err
	}
	if parsed == nil || len(parsed.List) == 0 {
		return nil, &UnresolvedProjectionError{}
	}
	p.SQL = source
	parts, ok := newSelectProjectionSource(source)
	columns, wildcard, err := p.columnsFromParsed(parsed, parts, ok, true)
	if err != nil {
		return nil, err
	}
	result := &ProjectionMembership{columns: columns, wildcard: wildcard, index: projectionColumnIndex(columns), sourceOnly: map[string]bool{}}
	for _, column := range columns {
		for _, name := range column.sourceOnly {
			result.sourceOnly[canonicalProjectionName(name)] = true
		}
	}
	return result, nil
}

// Columns validates one final response selection without parsing SQL or
// evaluating templates. Returned columns are detached from the retained slice.
func (p *ProjectionMembership) Columns(selected []string) ([]ProjectionColumn, error) {
	if p == nil {
		return nil, fmt.Errorf("projection membership is not checked")
	}
	columns, err := selectProjectionColumns(p.columns, selected, p.wildcard, p.index)
	if unknown, ok := err.(*UnknownProjectionColumnError); ok {
		for _, name := range selected {
			if canonicalProjectionName(name) == canonicalProjectionName(unknown.Column) {
				unknown.RequestedColumn = name
				break
			}
		}
	}
	return append([]ProjectionColumn(nil), columns...), err
}

// SourceOnlySelection uses the same verified aliases as SQL selection.
func (p *ProjectionMembership) SourceOnlySelection(name string) bool {
	return p != nil && p.sourceOnly[canonicalProjectionName(name)]
}

// ResponseFieldSelection is invocation-owned proof for response names that
// require an evaluated source. Concurrent relation batches share only checked
// alias classification, never SQL, rows, input, or template values.
type ResponseFieldSelection struct {
	fields     []string
	mu         sync.Mutex
	checked    bool
	sourceOnly map[string]bool
}

func NewResponseFieldSelection(fields []string) *ResponseFieldSelection {
	return &ResponseFieldSelection{fields: append([]string(nil), fields...)}
}

// ResponseProjectionConsistencyError means one view's evaluated batches
// disagree about a requested name's public output identity.
type ResponseProjectionConsistencyError struct{ Column string }

func (e *ResponseProjectionConsistencyError) Error() string {
	return fmt.Sprintf("response projection field %q has inconsistent output identity across evaluated view batches", e.Column)
}

// Validate checks all response names against the same native evaluated columns
// and retains only their verified source-only identity for the response mask.
func (s *ResponseFieldSelection) Validate(projection SelectorProjection) error {
	if s == nil || len(s.fields) == 0 {
		return nil
	}
	columns, wildcard, err := projection.columns()
	if err != nil {
		return err
	}
	return s.validateColumns(columns, wildcard)
}

func (s *ResponseFieldSelection) validateColumns(columns []ProjectionColumn, wildcard bool) error {
	if s == nil || len(s.fields) == 0 {
		return nil
	}
	membership := projectionColumnIndex(columns)
	if _, err := selectProjectionColumns(columns, s.fields, wildcard, membership); err != nil {
		if unknown, ok := err.(*UnknownProjectionColumnError); ok {
			for _, name := range s.fields {
				if (ProjectionNames{unknown.Column}).Matches(name) {
					unknown.RequestedColumn = name
					break
				}
			}
		}
		return err
	}
	sourceOnly := make(map[string]bool, len(s.fields))
	for _, name := range s.fields {
		key := canonicalProjectionName(name)
		if index, exists := membership[key]; exists && index >= 0 {
			sourceOnly[key] = columns[index].sourceOnly.Matches(name)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checked {
		for _, name := range s.fields {
			key := canonicalProjectionName(name)
			if s.sourceOnly[key] != sourceOnly[key] {
				return &ResponseProjectionConsistencyError{Column: name}
			}
		}
		return nil
	}
	s.sourceOnly = sourceOnly
	s.checked = true
	return nil
}

// SourceOnlySelection consumes successful evaluated proof after the read.
func (s *ResponseFieldSelection) SourceOnlySelection(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.checked {
		return false, fmt.Errorf("response projection membership has not been evaluated")
	}
	return s.sourceOnly[canonicalProjectionName(name)], nil
}

// Evaluated reports whether the native builder established membership. Row
// absence requires separate native collector evidence, since hooks can seed rows.
func (s *ResponseFieldSelection) Evaluated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checked
}
