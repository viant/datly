package dql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	sqltext "github.com/viant/sqlparser/source"
)

type declarationOptions struct {
	declarationSQL                                         string
	typeExpr                                               string
	tag                                                    string
	cardinality                                            string
	required                                               *bool
	cacheable                                              *bool
	minAllowedRecords, maxAllowedRecords, expectedReturned *int
	when                                                   string
	scope                                                  string
	with                                                   string
	uri                                                    string
	mcp                                                    *bool
	pathMCP                                                *bool
	value                                                  *string
	async                                                  bool
	errorStatusCode                                        int
	errorMessage                                           string
	description                                            string
	example                                                string
	emitOutput                                             bool
	predicates                                             []*spec.Predicate
	codec                                                  *spec.Codec
	querySelector                                          *spec.QuerySelectorBinding
	view                                                   declarationViewOptions
}

type declarationOptionParser struct {
	paramName string
	tail      string
	base      int
	view      bool
	seen      map[string]bool
	result    declarationOptions
}

func (p *declarationOptionParser) parse() (declarationOptions, error) {
	p.seen = map[string]bool{}
	cursor := newOptionCursor(p.tail)
	for cursor.next() {
		name, args := cursor.option()
		key := strings.ToLower(strings.TrimSpace(name))
		switch key {
		case "withmcp", "withpathmcp":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			value, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				return declarationOptions{}, p.fail(cursor, "%s requires true or false", name)
			}
			if key == "withmcp" {
				p.result.mcp = &value
			} else {
				p.result.pathMCP = &value
			}
		case "withuri":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.uri = strings.TrimSpace(trimQuote(args[0]))
			if p.result.uri == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a URI", name)
			}
		case "withconnector", "connector":
			if err := p.viewSingle(cursor, "view.connector", name, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.view.connector = strings.TrimSpace(trimQuote(args[0]))
			if p.result.view.connector == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a connector", name)
			}
		case "withtypename", "typename", "type":
			if err := p.viewSingle(cursor, "view.typeName", name, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.view.typeName = strings.TrimSpace(trimQuote(args[0]))
			if p.result.view.typeName == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a type name", name)
			}
		case "withdest", "dest":
			if err := p.viewSingle(cursor, "view.dest", name, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.view.dest = strings.TrimSpace(trimQuote(args[0]))
			if p.result.view.dest == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a destination", name)
			}
		case "withcache":
			if err := p.viewSingle(cursor, "view.cache", name, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.view.cache = strings.TrimSpace(trimQuote(args[0]))
			if p.result.view.cache == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a cache name", name)
			}
		case "withlimit":
			if err := p.viewSingle(cursor, "view.limit", name, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			value, ok := parseIntArg(args[0])
			if !ok || value < 0 {
				return declarationOptions{}, p.fail(cursor, "%s requires a non-negative integer", name)
			}
			p.result.view.limit = &value
		case "withcolumntype", "columntype":
			if err := p.viewColumn(cursor, "type", name, args); err != nil {
				return declarationOptions{}, err
			}
			column := p.result.view.column(trimQuote(args[0]))
			column.typeExpr = strings.TrimSpace(trimQuote(args[1]))
			column.typeOffset = p.base + cursor.start
			if column.typeExpr == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a column type", name)
			}
		case "withcolumntag", "columntag":
			if err := p.viewColumn(cursor, "tag", name, args); err != nil {
				return declarationOptions{}, err
			}
			column := p.result.view.column(trimQuote(args[0]))
			column.tag = strings.TrimSpace(trimQuote(args[1]))
			if column.tag == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires column tag metadata", name)
			}
		case "withcolumngroupable", "columngroupable":
			if err := p.viewColumn(cursor, "groupable", name, args); err != nil {
				return declarationOptions{}, err
			}
			value, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[1])))
			if err != nil {
				return declarationOptions{}, p.fail(cursor, "%s requires true or false", name)
			}
			column := p.result.view.column(trimQuote(args[0]))
			column.groupable = &value
		case "withtag", "tag":
			if err := p.single(cursor, "tag", args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.tag = strings.TrimSpace(trimQuote(args[0]))
			if p.result.tag == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires tag metadata", name)
			}
		case "optional", "required":
			if err := p.single(cursor, "required", args, 0, 0); err != nil {
				return declarationOptions{}, err
			}
			value := key == "required"
			p.result.required = &value
		case "cacheable":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			value, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				return declarationOptions{}, p.fail(cursor, "%s requires true or false", name)
			}
			p.result.cacheable = &value
		case "queryselector":
			if p.seen[key] {
				return declarationOptions{}, p.fail(cursor, "parameter %s has duplicate %s options", p.paramName, name)
			}
			p.seen[key] = true
			if len(args) != 1 {
				return declarationOptions{}, p.fail(cursor, "parameter %s QuerySelector requires exactly one view name", p.paramName)
			}
			property, supported := spec.SelectorPropertyForParam(p.paramName)
			viewName := strings.TrimSpace(trimQuote(args[0]))
			if !supported {
				return declarationOptions{}, p.fail(cursor, "parameter %s cannot be used as a query selector", p.paramName)
			}
			if viewName == "" {
				return declarationOptions{}, p.fail(cursor, "parameter %s QuerySelector requires a view name", p.paramName)
			}
			p.result.querySelector = &spec.QuerySelectorBinding{View: viewName, Property: property}
			if p.result.cacheable == nil {
				cacheable := false
				p.result.cacheable = &cacheable
			}
		case "withpredicate", "predicate", "applywhenabsentpredicate":
			if err := p.expect(cursor, args, 1, -1); err != nil {
				return declarationOptions{}, err
			}
			predicate := p.predicate(args, key == "applywhenabsentpredicate")
			if predicate == nil || strings.TrimSpace(predicate.Name) == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a predicate name", name)
			}
			p.result.predicates = append(p.result.predicates, predicate)
		case "when":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.when = strings.TrimSpace(trimQuote(args[0]))
			if p.result.when == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a condition", name)
			}
		case "scope":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.scope = strings.TrimSpace(trimQuote(args[0]))
			if p.result.scope == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a scope", name)
			}
		case "of":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.with = strings.TrimSpace(trimQuote(args[0]))
			if p.result.with == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a group name", name)
			}
		case "withtype":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.typeExpr = strings.TrimSpace(trimQuote(args[0]))
			if p.result.typeExpr == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a type", name)
			}
		case "withcodec":
			if err := p.single(cursor, key, args, 1, -1); err != nil {
				return declarationOptions{}, err
			}
			codecName := strings.TrimSpace(trimQuote(args[0]))
			if codecName == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires a codec name", name)
			}
			p.result.codec = &spec.Codec{Body: codecName}
			for _, arg := range args[1:] {
				p.result.codec.Args = append(p.result.codec.Args, trimQuote(arg))
			}
		case "withstatuscode":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			value, ok := parseIntArg(args[0])
			if !ok || value < 100 || value > 599 {
				return declarationOptions{}, p.fail(cursor, "%s requires an HTTP status code from 100 through 599", name)
			}
			p.result.errorStatusCode = value
		case "minallowedrecords", "maxallowedrecords", "expectedreturned":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			value, ok := parseIntArg(args[0])
			if !ok || value < 0 {
				return declarationOptions{}, p.fail(cursor, "%s requires a nonnegative integer", name)
			}
			switch key {
			case "minallowedrecords":
				p.result.minAllowedRecords = &value
			case "maxallowedrecords":
				p.result.maxAllowedRecords = &value
			case "expectedreturned":
				p.result.expectedReturned = &value
			}
		case "witherrormessage":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.errorMessage = trimQuote(args[0])
		case "withdescription", "description":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.description = strings.TrimSpace(trimQuote(args[0]))
			if p.result.description == "" {
				return declarationOptions{}, p.fail(cursor, "%s requires description text", name)
			}
		case "withexample", "example":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			p.result.example = trimQuote(args[0])
		case "value":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			value := trimQuote(args[0])
			p.result.value = &value
		case "embed":
			if err := p.single(cursor, key, args, 0, 0); err != nil {
				return declarationOptions{}, err
			}
			if !strings.Contains(p.result.tag, `anonymous:"true"`) {
				if strings.TrimSpace(p.result.tag) != "" {
					p.result.tag += " "
				}
				p.result.tag += `anonymous:"true"`
			}
		case "cardinality":
			if err := p.single(cursor, key, args, 1, 1); err != nil {
				return declarationOptions{}, err
			}
			switch strings.ToLower(strings.TrimSpace(trimQuote(args[0]))) {
			case "one":
				p.result.cardinality = "One"
			case "many":
				p.result.cardinality = "Many"
			default:
				return declarationOptions{}, p.fail(cursor, "%s supports only One or Many", name)
			}
		case "async":
			if err := p.single(cursor, key, args, 0, 0); err != nil {
				return declarationOptions{}, err
			}
			p.result.async = true
		case "output":
			if err := p.single(cursor, key, args, 0, 0); err != nil {
				return declarationOptions{}, err
			}
			p.result.emitOutput = true
		default:
			return declarationOptions{}, p.fail(cursor, "parameter %s has unsupported declaration option %s", p.paramName, name)
		}
	}
	if err := cursor.Err(); err != nil {
		return declarationOptions{}, p.fail(cursor, "%v", err)
	}
	if err := p.parseDeclarationSQL(cursor); err != nil {
		return declarationOptions{}, err
	}
	return p.result, nil
}

func (p *declarationOptionParser) parseDeclarationSQL(cursor *optionCursor) error {
	for cursor.cursor < len(p.tail) && sqltext.IsWhitespace(p.tail[cursor.cursor]) {
		cursor.cursor++
	}
	if cursor.cursor == len(p.tail) {
		return nil
	}
	cursor.start = cursor.cursor
	if !strings.HasPrefix(p.tail[cursor.cursor:], "/*") {
		cursor.cursor = len(p.tail)
		return p.fail(cursor, "parameter %s has unsupported declaration syntax", p.paramName)
	}

	contentStart := cursor.cursor + 2
	depth := 1
	for cursor.cursor = contentStart; cursor.cursor+1 < len(p.tail); cursor.cursor++ {
		switch p.tail[cursor.cursor : cursor.cursor+2] {
		case "/*":
			depth++
			cursor.cursor++
		case "*/":
			depth--
			if depth != 0 {
				cursor.cursor++
				continue
			}
			p.result.declarationSQL = strings.TrimSpace(p.tail[contentStart:cursor.cursor])
			cursor.cursor += 2
			for cursor.cursor < len(p.tail) && sqltext.IsWhitespace(p.tail[cursor.cursor]) {
				cursor.cursor++
			}
			if cursor.cursor == len(p.tail) {
				return nil
			}
			cursor.start = cursor.cursor
			cursor.cursor = len(p.tail)
			return p.fail(cursor, "parameter %s has unsupported syntax after declaration SQL", p.paramName)
		}
	}
	cursor.cursor = len(p.tail)
	return p.fail(cursor, "unclosed declaration SQL comment")
}

func (p *declarationOptionParser) single(cursor *optionCursor, key string, args []string, min, max int) error {
	if p.seen[key] {
		return p.fail(cursor, "parameter %s has duplicate %s options", p.paramName, cursor.name)
	}
	p.seen[key] = true
	return p.expect(cursor, args, min, max)
}

func (p *declarationOptionParser) viewSingle(cursor *optionCursor, key, name string, args []string, min, max int) error {
	if !p.view {
		return p.fail(cursor, "parameter %s cannot use view option %s", p.paramName, name)
	}
	p.result.view.mark(p.base + cursor.start)
	return p.single(cursor, key, args, min, max)
}

func (p *declarationOptionParser) viewColumn(cursor *optionCursor, facet, name string, args []string) error {
	if !p.view {
		return p.fail(cursor, "parameter %s cannot use view option %s", p.paramName, name)
	}
	p.result.view.mark(p.base + cursor.start)
	if err := p.expect(cursor, args, 2, 2); err != nil {
		return err
	}
	columnName := strings.TrimSpace(trimQuote(args[0]))
	if columnName == "" {
		return p.fail(cursor, "%s requires a column name", name)
	}
	key := "view.column." + strings.ToLower(columnName) + "." + facet
	if p.seen[key] {
		return p.fail(cursor, "parameter %s has duplicate %s for column %s", p.paramName, name, columnName)
	}
	p.seen[key] = true
	return nil
}

func (p *declarationOptionParser) expect(cursor *optionCursor, args []string, min, max int) error {
	if len(args) < min {
		return p.fail(cursor, "%s expects at least %d argument(s), got %d", cursor.name, min, len(args))
	}
	if max >= 0 && len(args) > max {
		return p.fail(cursor, "%s expects at most %d argument(s), got %d", cursor.name, max, len(args))
	}
	return nil
}

func (p *declarationOptionParser) fail(cursor *optionCursor, format string, args ...any) error {
	start := cursor.start
	end := cursor.cursor
	if end <= start {
		end = start + 1
	}
	return &sourceError{offset: p.base + start, end: p.base + end, err: fmt.Errorf(format, args...)}
}

func (p *declarationOptionParser) predicate(args []string, applyWhenAbsent bool) *spec.Predicate {
	if len(args) == 0 {
		return nil
	}
	group := 0
	nameIndex := 0
	if len(args) >= 2 {
		if parsed, ok := parseIntArg(args[0]); ok {
			group = parsed
			nameIndex = 1
		}
	}
	if len(args) <= nameIndex {
		return nil
	}
	result := &spec.Predicate{Group: group, Name: trimQuote(args[nameIndex]), ApplyWhenAbsent: applyWhenAbsent}
	for _, arg := range args[nameIndex+1:] {
		result.Args = append(result.Args, trimQuote(arg))
	}
	return result
}
