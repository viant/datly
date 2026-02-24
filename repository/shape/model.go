package shape

import (
	"reflect"

	"github.com/viant/datly/view"
	"github.com/viant/x"
)

// Mode controls which execution flow is expected from the shape pipeline.
type Mode string

const (
	ModeUnspecified Mode = ""
	ModeStruct      Mode = "struct"
	ModeDQL         Mode = "dql"
)

// Source represents the caller-provided shape source.
type Source struct {
	Name         string
	Path         string
	Connector    string
	Struct       any
	Type         reflect.Type
	TypeName     string
	TypeRegistry *x.Registry
	DQL          string
}

// ScanResult is the output produced by Scanner.
type ScanResult struct {
	Source      *Source
	Descriptors any
}

// PlanResult is the output produced by Planner.
type PlanResult struct {
	Source *Source
	Plan   any
}

// ViewArtifacts is the runtime view payload produced by Loader.
type ViewArtifacts struct {
	Resource *view.Resource
	Views    view.Views
}

// ComponentArtifact is the runtime component payload produced by Loader.
// Component stays untyped in the skeleton to avoid coupling shape package
// to repository internals before the implementation phase.
type ComponentArtifact struct {
	Resource  *view.Resource
	Component any
}
