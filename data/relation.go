package data

import "strings"

type Link struct {
	Namespace string
	Column    string
	Field     string
}

func NewLink(namespace, column, field string) *Link {
	return &Link{Namespace: namespace, Column: column, Field: field}
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
