package dql

import (
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
)

type componentSettings struct {
	SequenceStrategy           string
	MCPFolders                 []spec.ResourceFolder
	Documentation              xdocs.Source
	Static                     *spec.StaticContent
	IgnoreEmptyQueryParameters *bool
	DefaultConnector           string
	Report                     *spec.ReportSettings
	Cache                      *spec.CacheSettings
	Generation                 spec.GenerationSettings
	MCP                        *spec.MCPExposure
	MCPOnly                    bool
	InputType                  string
	OutputType                 string
	JSONMarshalType            string
	JSONUnmarshalType          string
	XMLUnmarshalType           string
	Format                     string
	DateFormat                 string
	CaseFormat                 string
	Output                     *spec.OutputSettings
	Const                      map[string]string
	constSpans                 map[string]SourceSpan
	mcpSpan                    SourceSpan
}
