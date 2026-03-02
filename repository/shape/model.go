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

// ScanSpec is implemented by every scan-pipeline descriptor result.
// The sole production implementation is *scan.Result.
type ScanSpec interface {
	// ShapeSpecKind returns a diagnostic label used in error messages.
	ShapeSpecKind() string
}

// PlanSpec is implemented by every plan-pipeline result.
// The sole production implementation is *plan.Result.
type PlanSpec interface {
	// ShapeSpecKind returns a diagnostic label used in error messages.
	ShapeSpecKind() string
}

// ComponentSpec is implemented by every component loader result.
// The sole production implementation is *load.Component.
type ComponentSpec interface {
	// ShapeSpecKind returns a diagnostic label used in error messages.
	ShapeSpecKind() string
}

// ScanResult is the output produced by Scanner.
type ScanResult struct {
	Source      *Source
	Descriptors ScanSpec
}

// PlanResult is the output produced by Planner.
type PlanResult struct {
	Source *Source
	Plan   PlanSpec
}

// ViewArtifacts is the runtime view payload produced by Loader.
type ViewArtifacts struct {
	Resource *view.Resource
	Views    view.Views
}

// ComponentArtifact is the runtime component payload produced by Loader.
type ComponentArtifact struct {
	Resource  *view.Resource
	Component ComponentSpec
}
