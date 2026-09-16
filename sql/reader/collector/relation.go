package collector

import (
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/xunsafe"
)

const (
	MatchSequential = data.MatchSequential
	MatchReadAll    = data.MatchReadAll
)

// View is reader-compiled state for one immutable data view.
type View struct {
	*data.View
	Schema    Schema
	Relations []*Relation
	Tree      *TreePlan
}

// Relation is reader-compiled state for one metadata relation.
type Relation struct {
	*data.Relation
	On          Links
	Of          *RelationRef
	HolderField *xunsafe.Field
	HolderSlice *xunsafe.Slice
}

type RelationRef struct {
	*data.RelationRef
	View *View
	On   Links
}

type Link struct {
	*data.Link
	// KeySource is resolved once at compile time, never from a row's value.
	KeySource KeySource
	// XField is present only for typed field and hook sources. Every key
	// consumer uses this accessor or the captured SQL column, consistently.
	XField *xunsafe.Field
}

type KeySource string

const (
	KeySourceField  KeySource = "field"
	KeySourceColumn KeySource = "column"
	KeySourceHook   KeySource = "hook"
)

type Links []*Link

func (l Links) InColumnExpression() []string {
	result := make([]string, 0, len(l))
	for _, link := range l {
		if link != nil && link.Link != nil {
			column := link.Column
			if namespace := strings.TrimSpace(link.Namespace); namespace != "" {
				column = namespace + "." + column
			}
			result = append(result, column)
		}
	}
	return result
}

type Relations []*Relation

func (r Relations) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0, len(r))
	for _, relation := range r {
		if relation != nil {
			result = append(result, relation)
		}
	}
	return result
}
