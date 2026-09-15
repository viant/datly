package data

import "strings"

type Link struct {
	Namespace string
	// Column addresses the SQL source used by a matching predicate.
	Column string
	// Output retains the authored result label when it differs from Column.
	Output string
	Field  string
}

func NewLink(namespace, column, field string) *Link {
	return &Link{Namespace: namespace, Column: column, Field: field}
}

// OutputColumn is the result value required when this relation is selected.
func (l *Link) OutputColumn() string {
	if l == nil {
		return ""
	}
	if l.Output != "" {
		return l.Output
	}
	if l.Column != "" {
		return l.Column
	}
	return l.Field
}

type Links []*Link

func (l Links) InColumnExpression() []string {
	result := make([]string, 0, len(l))
	for _, link := range l {
		if link != nil {
			column := link.Column
			if namespace := strings.TrimSpace(link.Namespace); namespace != "" {
				column = namespace + "." + column
			}
			result = append(result, column)
		}
	}
	return result
}

type MatchStrategy int

const (
	MatchSequential MatchStrategy = iota
	MatchReadAll
)

func (s MatchStrategy) ReadAll() bool { return s == MatchReadAll }

type RelationRef struct {
	View          *View
	On            Links
	MatchStrategy MatchStrategy
}

func (r *Relation) IsComposite() bool {
	return r != nil && len(r.On) > 1
}

type Relations []*Relation

func (r Relations) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0, len(r))
	for i := range r {
		result = append(result, r[i])
	}
	return result
}
