package gorouter

import "github.com/viant/datly/repository/shape"

// RouteSource represents one component route field discovered from a Go source package.
type RouteSource struct {
	Name        string
	FieldName   string
	FilePath    string
	PackageName string
	PackagePath string
	PackageDir  string
	RoutePath   string
	Method      string
	Connector   string
	InputRef    string
	OutputRef   string
	ViewRef     string
	SourceURL   string
	SummaryURL  string
	Source      *shape.Source
}
