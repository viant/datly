package ast

import "github.com/viant/datly/spec"

// ReadCollection describes an ordinary bound read for application lookup.
// It grants no write permission; CurrentPlan owns mutation matching scope.
type ReadCollection struct {
	Name      string
	InputPath FieldPath
	Type      spec.TypeRef
	Fields    []FieldRef
	Keys      []KeyPart
	// Groups are exact canonical relationship tuples prepared automatically.
	Groups []ReadGroup
}

// ReadGroup is a typed relationship-key tuple, not an inferred naming convention.
type ReadGroup struct{ Parts []KeyPart }
