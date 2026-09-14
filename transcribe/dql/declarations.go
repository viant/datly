package dql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

func parseDeclarations(blocks []directiveBlock) (result []*spec.Parameter, spans map[string]SourceSpan, views map[string]declarationViewOptions, err error) {
	var active directiveBlock
	defer func() { err = wrapDirectiveError(err, active) }()
	indexByKey := map[string]int{}
	for _, block := range blocks {
		if isCubeSetting(block) {
			continue
		}
		declarationKind, ok := toDeclarationKind(block.kind)
		if !ok {
			continue
		}
		active = block
		holder, kind, location, tail, tailOffset, ok := parseDeclarationHead(block.body)
		implicit := false
		if !ok {
			holder, tail, tailOffset, ok = parseImplicitDeclarationHead(block.body)
			if !ok {
				if declarationKind == spec.DeclarationKindDefine {
					return nil, nil, nil, fmt.Errorf("invalid #define declaration: expected $Name<Type>(kind/location)")
				}
				continue
			}
			location = holder
			implicit = true
		}
		inputType, outputType := parseDeclarationTypes(block.body)
		optionParser := declarationOptionParser{
			paramName: holder,
			tail:      tail,
			base:      block.bodyStart + tailOffset,
			view:      implicit || isViewDeclarationKind(kind),
		}
		options, err := optionParser.parse()
		if err != nil {
			return nil, nil, nil, err
		}
		declarationSQL := options.declarationSQL
		if implicit && declarationSQL == "" {
			if declarationKind == spec.DeclarationKindDefine {
				return nil, nil, nil, fmt.Errorf("invalid #define declaration: implicit declarations require SQL")
			}
			continue
		}
		if kind == "" {
			kind, err = inferImplicitDeclarationKind(declarationSQL)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		if strings.EqualFold(kind, "param") && declarationSQL != "" && options.codec == nil {
			analysis, analyzeErr := AnalyzeDeclarationSQL(declarationSQL)
			if analyzeErr == nil && analysis != nil && (isStructQLPath(analysis.FromPath) || isStructQLPath(analysis.Table)) {
				options.codec = &spec.Codec{Body: "structql", Args: []string{analysis.SQL}}
			}
		}
		if implicit && options.view.used && !isViewDeclarationKind(kind) {
			return nil, nil, nil, &sourceError{
				offset: options.view.firstOffset, end: options.view.firstOffset + 1,
				err: fmt.Errorf("parameter %s cannot use view options for inferred %s declaration", holder, kind),
			}
		}
		if strings.EqualFold(kind, "view") || strings.EqualFold(kind, "data_view") {
			// One provider kind owns independent reads. The declaration holder is
			// their canonical lookup identity, matching the original locator model.
			kind = "view"
			location = holder
		}
		options.view.materialize = strings.EqualFold(kind, "view") && (!implicit || options.view.used || options.uri != "")
		if options.typeExpr != "" {
			inputType = options.typeExpr
		}
		required := options.required
		if required == nil {
			switch strings.ToLower(strings.TrimSpace(kind)) {
			case "query":
				value := false
				required = &value
			case "header":
				value := true
				required = &value
			}
		}
		activation, resourceRef := declarationURI(options.uri)
		param := &spec.Parameter{
			Name:        holder,
			Declaration: declarationKind,
			Source: spec.BindSource{
				Kind: kind,
				Name: location,
			},
			TypeExpr:          inputType,
			OutputTypeExpr:    outputType,
			DeclarationSQL:    declarationSQL,
			Tag:               options.tag,
			Cardinality:       options.cardinality,
			Required:          required,
			Cacheable:         options.cacheable,
			MinAllowedRecords: options.minAllowedRecords, MaxAllowedRecords: options.maxAllowedRecords, ExpectedReturned: options.expectedReturned,
			When:       options.when,
			Scope:      options.scope,
			With:       options.with,
			Activation: activation,
			MCP:        options.mcp, PathMCP: options.pathMCP,
			ResourceRef:     resourceRef,
			Value:           options.value,
			Async:           options.async,
			ErrorStatusCode: options.errorStatusCode,
			ErrorMessage:    options.errorMessage,
			EmitOutput:      options.emitOutput,
			Predicates:      options.predicates,
			Codec:           options.codec,
			QuerySelector:   options.querySelector,
			Raw:             strings.TrimSpace(block.body),
		}
		key := param.Identity()
		if existingIndex, ok := indexByKey[key]; ok {
			existing := result[existingIndex]
			if existing != nil && existing.Declaration == spec.DeclarationKindDefine && declarationKind == spec.DeclarationKindDefine {
				return nil, nil, nil, fmt.Errorf("parameter %s is defined more than once", holder)
			}
			if existing != nil && existing.Declaration == spec.DeclarationKindSet && declarationKind == spec.DeclarationKindDefine {
				result[existingIndex] = param
				if spans == nil {
					spans = map[string]SourceSpan{}
				}
				spans[param.Identity()] = SourceSpan{Start: block.start, End: block.end}
				if views == nil {
					views = map[string]declarationViewOptions{}
				}
				views[param.Identity()] = options.view
			}
			continue
		}
		indexByKey[key] = len(result)
		result = append(result, param)
		if spans == nil {
			spans = map[string]SourceSpan{}
		}
		spans[param.Identity()] = SourceSpan{Start: block.start, End: block.end}
		if views == nil {
			views = map[string]declarationViewOptions{}
		}
		views[param.Identity()] = options.view
	}
	return result, spans, views, nil
}

func declarationURI(uri string) (*spec.RouteActivation, string) {
	uri = strings.TrimSpace(uri)
	if strings.HasPrefix(uri, "/") {
		return &spec.RouteActivation{URI: uri}, ""
	}
	return nil, uri
}

func isStructQLPath(value string) bool {
	value = strings.Trim(strings.TrimSpace(value), "`")
	return strings.HasPrefix(value, "/")
}

func isViewDeclarationKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "view", "data_view":
		return true
	default:
		return false
	}
}

func inferImplicitDeclarationKind(declarationSQL string) (string, error) {
	declarationSQL = strings.TrimSpace(declarationSQL)
	if declarationSQL == "" {
		return "", nil
	}
	analysis, err := AnalyzeDeclarationSQL(declarationSQL)
	if err != nil {
		return "", err
	}
	if analysis != nil && (isStructQLPath(analysis.FromPath) || isStructQLPath(analysis.Table)) {
		return "param", nil
	}
	return "view", nil
}

func toDeclarationKind(kind directiveKind) (spec.DeclarationKind, bool) {
	switch kind {
	case directiveKindDefine:
		return spec.DeclarationKindDefine, true
	case directiveKindSet:
		return spec.DeclarationKindSet, true
	default:
		return "", false
	}
}

func isDeclarationBlock(block directiveBlock) bool {
	if block.kind != directiveKindDefine && block.kind != directiveKindSet {
		return false
	}
	_, _, _, _, _, ok := parseDeclarationHead(block.body)
	if ok {
		return true
	}
	holder, tail, tailOffset, ok := parseImplicitDeclarationHead(block.body)
	if !ok {
		return false
	}
	options, err := (&declarationOptionParser{
		paramName: holder,
		tail:      tail,
		base:      block.bodyStart + tailOffset,
		view:      true,
	}).parse()
	return err == nil && options.declarationSQL != ""
}
