package spec

import xdocs "github.com/viant/xdatly/docs"

type Component struct {
	Static        *StaticContent `json:"static,omitempty"`
	Documentation xdocs.Source   `json:"documentation,omitempty"`
	Key           Key            `json:"key"`
	Name          string         `json:"name,omitempty"`
	Description   string         `json:"description,omitempty"`
	Example       string         `json:"example,omitempty"`
	Settings      *Settings      `json:"settings,omitempty"`
	TypeContext   *TypeContext   `json:"typeContext,omitempty"`
	Routes        []*Route       `json:"routes,omitempty"`
	Parameters    []*Parameter   `json:"params,omitempty"`
	RootView      *View          `json:"rootView,omitempty"`
	// Views contains independent DI-backed reads declared as view data points.
	// They are not root relations unless an authored relation explicitly links them.
	Views []*View `json:"views,omitempty"`
}

type TypeContext struct {
	// PackagePath is the destination explicitly authored by #package.
	PackagePath    string       `json:"packagePath,omitempty"`
	DefaultPackage string       `json:"defaultPackage,omitempty"`
	Imports        []ImportSpec `json:"imports,omitempty"`
}

type ImportSpec struct {
	Alias   string `json:"alias,omitempty"`
	Package string `json:"package,omitempty"`
}

type Settings struct {
	Mutation                   string              `json:"mutation,omitempty"`
	SequenceStrategy           string              `json:"sequenceStrategy,omitempty"`
	MCPFolders                 []ResourceFolder    `json:"mcpFolders,omitempty"`
	IgnoreEmptyQueryParameters *bool               `json:"ignoreEmptyQueryParameters,omitempty"`
	DefaultConnector           string              `json:"defaultConnector,omitempty"`
	Report                     *ReportSettings     `json:"report,omitempty"`
	Cache                      *CacheSettings      `json:"cache,omitempty"`
	Generation                 *GenerationSettings `json:"generation,omitempty"`
	InputType                  string              `json:"inputType,omitempty"`
	OutputType                 string              `json:"outputType,omitempty"`
	JSONMarshalType            string              `json:"jsonMarshalType,omitempty"`
	JSONUnmarshalType          string              `json:"jsonUnmarshalType,omitempty"`
	XMLUnmarshalType           string              `json:"xmlUnmarshalType,omitempty"`
	Format                     string              `json:"format,omitempty"`
	DateFormat                 string              `json:"dateFormat,omitempty"`
	Output                     *OutputSettings     `json:"output,omitempty"`
	CaseFormat                 string              `json:"caseFormat,omitempty"`
	Const                      map[string]string   `json:"const,omitempty"`
}

// OutputSettings describes presentation policy independently of the row shape.
type OutputSettings struct {
	Exclude   []string `json:"exclude,omitempty"`
	OmitEmpty bool     `json:"omitEmpty,omitempty"`
	Title     string   `json:"title,omitempty"`
}

// GenerationSettings contains transcription controls that are consumed while
// producing a Go package and are not part of runtime component behavior.
type GenerationSettings struct {
	FilePrefix          string            `json:"filePrefix,omitempty"`
	Template            string            `json:"template,omitempty"`
	DescriptionResource string            `json:"descriptionResource,omitempty"`
	ViewFile            string            `json:"viewFile,omitempty"`
	InputFile           string            `json:"inputFile,omitempty"`
	OutputFile          string            `json:"outputFile,omitempty"`
	RouterFile          string            `json:"routerFile,omitempty"`
	HandlerFile         string            `json:"handlerFile,omitempty"`
	LifecycleFile       string            `json:"lifecycleFile,omitempty"`
	MutationFile        string            `json:"mutationFile,omitempty"`
	ResourcesFile       string            `json:"resourcesFile,omitempty"`
	LinksFile           string            `json:"linksFile,omitempty"`
	TemplateFile        string            `json:"templateFile,omitempty"`
	SupportFiles        map[string]string `json:"supportFiles,omitempty"`
	SQLFiles            map[string]string `json:"sqlFiles,omitempty"`
}

func (s GenerationSettings) IsZero() bool {
	return s.FilePrefix == "" && s.Template == "" && s.DescriptionResource == "" && s.ViewFile == "" &&
		s.InputFile == "" && s.OutputFile == "" && s.RouterFile == "" &&
		s.HandlerFile == "" && s.LifecycleFile == "" && s.MutationFile == "" &&
		s.ResourcesFile == "" && s.LinksFile == "" && s.TemplateFile == "" && len(s.SupportFiles) == 0 && len(s.SQLFiles) == 0
}

type ReportSettings struct {
	Compose         *CubeComposeSettings `json:"compose,omitempty"`
	Enabled         bool                 `json:"enabled,omitempty"`
	MCPTool         *bool                `json:"mcpTool,omitempty"`
	LinkedInputType string               `json:"linkedInputType,omitempty"`
	InputLayout     *ReportInputLayout   `json:"inputLayout,omitempty"`
}

// ReportInputLayout maps report semantics to input struct fields. Generated
// report inputs use canonical field names by default and require no layout.
type ReportInputLayout struct {
	Dimensions string `json:"dimensions,omitempty"`
	Measures   string `json:"measures,omitempty"`
	Filters    string `json:"filters,omitempty"`
	OrderBy    string `json:"orderBy,omitempty"`
	Limit      string `json:"limit,omitempty"`
	Offset     string `json:"offset,omitempty"`
}

type CacheSettings struct {
	SleepBetweenRetriesInMs int                  `json:"sleepBetweenRetriesInMs,omitempty"`
	MaxRetries              int                  `json:"maxRetries,omitempty"`
	TotalTimeoutInMs        int                  `json:"totalTimeoutInMs,omitempty"`
	SocketTimeoutInMs       int                  `json:"socketTimeoutInMs,omitempty"`
	FailedRequestLimit      int                  `json:"failedRequestLimit,omitempty"`
	ResetFailuresInMs       int                  `json:"resetFailuresInMs,omitempty"`
	Enabled                 bool                 `json:"enabled,omitempty"`
	Name                    string               `json:"name,omitempty"`
	TTL                     string               `json:"ttl,omitempty"`
	Provider                string               `json:"provider,omitempty"`
	Location                string               `json:"location,omitempty"`
	TimeToLiveMs            int                  `json:"timeToLiveMs,omitempty"`
	Warmup                  *CacheWarmupSettings `json:"warmup,omitempty"`
}

type CacheWarmupSettings struct {
	Limit          *int               `json:"limit,omitempty"`
	MaxCases       *int               `json:"maxCases,omitempty"`
	FieldNames     []string           `json:"fieldNames,omitempty"`
	IndexColumn    string             `json:"indexColumn,omitempty"`
	IndexParameter string             `json:"indexParameter,omitempty"`
	IndexMeta      bool               `json:"indexMeta,omitempty"`
	Connector      string             `json:"connector,omitempty"`
	Cases          []*CacheWarmupCase `json:"cases,omitempty"`
}

type CacheWarmupCase struct {
	Set        []*CacheWarmupParam `json:"set,omitempty"`
	FieldNames []string            `json:"fieldNames,omitempty"`
}

type CacheWarmupParam struct {
	Name           string   `json:"name,omitempty"`
	Values         []string `json:"values,omitempty"`
	ExcludeDefault bool     `json:"excludeDefault,omitempty"`
}

type Route struct {
	CORS         *CORS          `json:"cors,omitempty"`
	Method       string         `json:"method"`
	Path         string         `json:"path"`
	Name         string         `json:"name,omitempty"`
	Internal     bool           `json:"internal,omitempty"`
	Marshaller   string         `json:"marshaller,omitempty"`
	Handler      string         `json:"handler,omitempty"`
	APIKeyHeader string         `json:"apiKeyHeader,omitempty"`
	APIKeyValue  string         `json:"apiKeyValue,omitempty"`
	MCP          []*MCPExposure `json:"mcp,omitempty"`
}

// IsZero reports whether any component settings are configured.
func (s *Settings) IsZero() bool {
	if s == nil {
		return true
	}
	return len(s.MCPFolders) == 0 && s.IgnoreEmptyQueryParameters == nil &&
		s.DefaultConnector == "" && s.SequenceStrategy == "" && s.Report == nil && s.Cache == nil &&
		(s.Generation == nil || s.Generation.IsZero()) && s.InputType == "" && s.OutputType == "" &&
		s.JSONMarshalType == "" && s.JSONUnmarshalType == "" && s.XMLUnmarshalType == "" &&
		s.Format == "" && s.DateFormat == "" && s.Output == nil && s.CaseFormat == "" && len(s.Const) == 0
}
