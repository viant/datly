package dql

import (
	"fmt"
	"go/token"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

func parseComponentSettings(blocks []directiveBlock) (ret *componentSettings, err error) {
	var active directiveBlock
	defer func() { err = wrapDirectiveError(err, active) }()
	ret = &componentSettings{}
	constantNames := map[string]string{}
	for _, block := range blocks {
		if block.kind != directiveKindSetting && !isCubeSetting(block) {
			continue
		}
		active = block
		name, args, tail, ok := parseDirectiveCall(block.body)
		if !ok {
			continue
		}
		switch {
		case strings.EqualFold(name, "mcp_folder"), strings.EqualFold(name, "mcp_skill_folder"):
			if len(args) != 3 || tail != "" {
				return nil, fmt.Errorf("mcp_folder requires namespace, root and URI prefix")
			}
			folder := spec.ResourceFolder{Namespace: trimQuote(args[0]), Root: trimQuote(args[1]), URIPrefix: trimQuote(args[2])}
			if strings.EqualFold(name, "mcp_skill_folder") {
				folder.Skills = []string{"."}
			}
			if err := folder.Validate(); err != nil {
				return nil, err
			}
			ret.MCPFolders = append(ret.MCPFolders, folder)
		case strings.EqualFold(name, "static_resource"), strings.EqualFold(name, "static_content"):
			if len(args) != 2 || tail != "" || ret.Static != nil {
				return nil, fmt.Errorf("%s requires exactly source and root, once", name)
			}
			source, sourceOK := parseQuotedLiteral(args[0])
			root, rootOK := parseQuotedLiteral(args[1])
			if !sourceOK || !rootOK {
				return nil, fmt.Errorf("%s arguments must be quoted literals", name)
			}
			ret.Static = &spec.StaticContent{Root: root}
			if strings.EqualFold(name, "static_resource") {
				ret.Static.Namespace = source
			} else {
				ret.Static.ContentURL = source
			}
		case strings.EqualFold(name, "useTemplate"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid useTemplate directive: missing template type")
			}
			ret.Generation.Template = strings.TrimSpace(trimQuote(args[len(args)-1]))
			if ret.Generation.Template == "" {
				return nil, fmt.Errorf("invalid useTemplate directive: empty template type")
			}
		case strings.EqualFold(name, "connector"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid connector directive: missing connector name")
			}
			ret.DefaultConnector = trimQuote(args[len(args)-1])
		case strings.EqualFold(name, "report"), strings.EqualFold(name, "cube"):
			report, err := parseReportSettings(name, args)
			if err != nil {
				return nil, err
			}
			if ret.Report != nil {
				report.Compose = ret.Report.Compose
			}
			ret.Report = report
		case strings.EqualFold(name, "cubeCompose"):
			if len(args) != 1 || tail != "" {
				return nil, fmt.Errorf("cubeCompose requires exactly one boolean")
			}
			enabled, parseErr := strconv.ParseBool(strings.TrimSpace(trimQuote(args[0])))
			if parseErr != nil {
				return nil, fmt.Errorf("cubeCompose requires true or false")
			}
			if ret.Report == nil {
				ret.Report = &spec.ReportSettings{}
			}
			ret.Report.Compose = &spec.CubeComposeSettings{Enabled: enabled}
		case strings.EqualFold(name, "cache"):
			cache, err := parseCacheSettings(args, tail)
			if err != nil {
				return nil, err
			}
			ret.Cache = cache
		case strings.EqualFold(name, "cache_warmup"):
			warmup, err := parseCacheWarmupSettings(args)
			if err != nil {
				return nil, err
			}
			if ret.Cache == nil {
				ret.Cache = &spec.CacheSettings{Enabled: true}
			}
			ret.Cache.Warmup, err = mergeCacheWarmupSettings(ret.Cache.Warmup, warmup)
			if err != nil {
				return nil, err
			}
		case strings.EqualFold(name, "DocGlobalURLs"), strings.EqualFold(name, "DocURL"), strings.EqualFold(name, "DocURLs"), strings.EqualFold(name, "DocBaseURL"):
			if len(args) == 0 || tail != "" {
				return nil, fmt.Errorf("%s requires resource references", name)
			}
			if !strings.EqualFold(name, "DocGlobalURLs") && !strings.EqualFold(name, "DocURLs") && len(args) != 1 {
				return nil, fmt.Errorf("%s requires exactly one value", name)
			}
			values := make([]string, len(args))
			for i, arg := range args {
				values[i] = strings.TrimSpace(trimQuote(arg))
				if values[i] == "" {
					return nil, fmt.Errorf("%s requires nonempty values", name)
				}
			}
			switch strings.ToLower(name) {
			case "docglobalurls":
				ret.Documentation.GlobalURLs = values
			case "docurl":
				ret.Documentation.DocURL = values[0]
			case "docurls":
				ret.Documentation.DocURLs = values
			case "docbaseurl":
				ret.Documentation.BaseURL = values[0]
			}
		case strings.EqualFold(name, "meta"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid meta directive: missing path")
			}
			ret.Generation.DescriptionResource = strings.TrimSpace(trimQuote(args[len(args)-1]))
			if ret.Generation.DescriptionResource == "" {
				return nil, fmt.Errorf("invalid meta directive: empty path")
			}
		case strings.EqualFold(name, "mcp"):
			mcp, err := parseMCPExposure(args)
			if err != nil {
				return nil, err
			}
			ret.MCP = mcp
			ret.mcpSpan = SourceSpan{Start: block.start, End: block.end}
		case strings.EqualFold(name, "dest"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid dest directive: missing destination")
			}
			ret.Generation.ViewFile = strings.TrimSpace(trimQuote(args[len(args)-1]))
			if ret.Generation.ViewFile == "" {
				return nil, fmt.Errorf("invalid dest directive: empty destination")
			}
		case strings.EqualFold(name, "input_dest"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid input_dest directive: missing destination")
			}
			ret.Generation.InputFile = strings.TrimSpace(trimQuote(args[len(args)-1]))
			if ret.Generation.InputFile == "" {
				return nil, fmt.Errorf("invalid input_dest directive: empty destination")
			}
		case strings.EqualFold(name, "output_dest"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid output_dest directive: missing destination")
			}
			ret.Generation.OutputFile = strings.TrimSpace(trimQuote(args[len(args)-1]))
			if ret.Generation.OutputFile == "" {
				return nil, fmt.Errorf("invalid output_dest directive: empty destination")
			}
		case strings.EqualFold(name, "router_dest"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid router_dest directive: missing destination")
			}
			ret.Generation.RouterFile = strings.TrimSpace(trimQuote(args[len(args)-1]))
			if ret.Generation.RouterFile == "" {
				return nil, fmt.Errorf("invalid router_dest directive: empty destination")
			}
		case strings.EqualFold(name, "file_prefix"), strings.EqualFold(name, "handler_dest"), strings.EqualFold(name, "lifecycle_dest"), strings.EqualFold(name, "mutation_dest"), strings.EqualFold(name, "resources_dest"), strings.EqualFold(name, "links_dest"), strings.EqualFold(name, "template_dest"), strings.EqualFold(name, "support_dest"):
			expected := 1
			if strings.EqualFold(name, "support_dest") {
				expected = 2
			}
			if len(args) != expected || tail != "" {
				return nil, fmt.Errorf("%s requires exactly %d quoted arguments", name, expected)
			}
			values := make([]string, len(args))
			for i, arg := range args {
				value, ok := parseQuotedLiteral(arg)
				if !ok || strings.TrimSpace(value) == "" {
					return nil, fmt.Errorf("%s requires nonempty quoted arguments", name)
				}
				values[i] = value
			}
			generation := &ret.Generation
			var destination *string
			switch strings.ToLower(name) {
			case "file_prefix":
				destination = &generation.FilePrefix
			case "handler_dest":
				destination = &generation.HandlerFile
			case "lifecycle_dest":
				destination = &generation.LifecycleFile
			case "mutation_dest":
				destination = &generation.MutationFile
			case "resources_dest":
				destination = &generation.ResourcesFile
			case "links_dest":
				destination = &generation.LinksFile
			case "template_dest":
				destination = &generation.TemplateFile
			case "support_dest":
				if err := generation.SetSupportFile(values[0], values[1]); err != nil {
					return nil, err
				}
			}
			if destination != nil {
				if *destination != "" {
					return nil, fmt.Errorf("duplicate %s directive", name)
				}
				*destination = values[0]
			}
		case strings.EqualFold(name, "input_type"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid input_type directive: missing type")
			}
			ret.InputType = trimQuote(args[len(args)-1])
		case strings.EqualFold(name, "output_type"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid output_type directive: missing type")
			}
			ret.OutputType = trimQuote(args[len(args)-1])
		case strings.EqualFold(name, "marshal"):
			mime, typeName, err := parseMarshalLikeDirective("marshal", args)
			if err != nil {
				return nil, err
			}
			if strings.EqualFold(mime, "application/json") {
				ret.JSONMarshalType = typeName
			}
		case strings.EqualFold(name, "unmarshal"):
			mime, typeName, err := parseMarshalLikeDirective("unmarshal", args)
			if err != nil {
				return nil, err
			}
			switch strings.ToLower(strings.TrimSpace(mime)) {
			case "application/json":
				ret.JSONUnmarshalType = typeName
			case "application/xml":
				ret.XMLUnmarshalType = typeName
			}
		case strings.EqualFold(name, "format"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid format directive: missing format")
			}
			ret.Format = normalizeFormat(trimQuote(args[len(args)-1]))
		case strings.EqualFold(name, "ignoreEmptyQueryParameters"):
			if len(args) != 1 {
				return nil, fmt.Errorf("ignoreEmptyQueryParameters requires one boolean")
			}
			value, err := strconv.ParseBool(trimQuote(args[0]))
			if err != nil {
				return nil, fmt.Errorf("ignoreEmptyQueryParameters requires true or false")
			}
			ret.IgnoreEmptyQueryParameters = &value
		case strings.EqualFold(name, "date_format"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid date_format directive: missing format")
			}
			ret.DateFormat = trimQuote(args[len(args)-1])
		case strings.EqualFold(name, "case_format"):
			if len(args) == 0 {
				return nil, fmt.Errorf("invalid case_format directive: missing format")
			}
			ret.CaseFormat = trimQuote(args[len(args)-1])
		case strings.EqualFold(name, "output_exclude"):
			if len(args) == 0 {
				return nil, fmt.Errorf("output_exclude requires at least one field path")
			}
			if ret.Output == nil {
				ret.Output = &spec.OutputSettings{}
			}
			for _, argument := range args {
				ret.Output.Exclude = append(ret.Output.Exclude, trimQuote(argument))
			}
		case strings.EqualFold(name, "output_omit_empty"):
			if len(args) != 1 {
				return nil, fmt.Errorf("output_omit_empty requires one boolean")
			}
			enabled, err := strconv.ParseBool(trimQuote(args[0]))
			if err != nil {
				return nil, err
			}
			if ret.Output == nil {
				ret.Output = &spec.OutputSettings{}
			}
			ret.Output.OmitEmpty = enabled
		case strings.EqualFold(name, "output_title"):
			if len(args) != 1 {
				return nil, fmt.Errorf("output_title requires one title")
			}
			if ret.Output == nil {
				ret.Output = &spec.OutputSettings{}
			}
			ret.Output.Title = trimQuote(args[0])
		case strings.EqualFold(name, "const"):
			if len(args) < 2 {
				return nil, fmt.Errorf("invalid const directive: expected name and value")
			}
			constantName := strings.TrimSpace(trimQuote(args[0]))
			if !token.IsIdentifier(constantName) {
				return nil, fmt.Errorf("invalid const directive: name %q must be a Go identifier", constantName)
			}
			canonicalName := strings.ToLower(constantName)
			if previous := constantNames[canonicalName]; previous != "" {
				return nil, fmt.Errorf("constant names %q and %q are ambiguous", previous, constantName)
			}
			constantNames[canonicalName] = constantName
			if ret.Const == nil {
				ret.Const = map[string]string{}
				ret.constSpans = map[string]SourceSpan{}
			}
			ret.Const[constantName] = trimQuote(args[1])
			ret.constSpans[canonicalName] = SourceSpan{Start: block.start, End: block.end}
		}
	}
	if err := ret.Generation.ValidateFilePrefix(); err != nil {
		return nil, err
	}
	if ret.Static == nil && len(ret.MCPFolders) == 0 && ret.Documentation.IsZero() && ret.Generation.IsZero() && ret.DefaultConnector == "" && ret.Report == nil && ret.Cache == nil &&
		ret.InputType == "" && ret.OutputType == "" &&
		ret.MCP == nil && ret.JSONMarshalType == "" &&
		ret.JSONUnmarshalType == "" && ret.XMLUnmarshalType == "" &&
		ret.Format == "" && ret.DateFormat == "" && ret.CaseFormat == "" && ret.Output == nil &&
		len(ret.Const) == 0 && ret.IgnoreEmptyQueryParameters == nil {
		return nil, nil
	}
	return ret, nil
}
