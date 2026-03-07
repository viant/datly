package compile

import (
	"fmt"
	"strconv"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view/extension"
	st "github.com/viant/datly/view/state"
	"github.com/viant/parsly"
)

func appendDeclaredStates(rawDQL string, result *plan.Result) {
	if result == nil || strings.TrimSpace(rawDQL) == "" {
		return
	}
	seen := map[string]bool{}
	for _, block := range extractSetBlocks(rawDQL) {
		holder, kind, location, tail, tailOffset, ok := parseSetDeclarationBody(block.Body)
		if !ok {
			continue
		}
		key := declaredStateKey(holder, kind, location)
		if seen[key] {
			continue
		}
		inName := location
		if kind == "view" || kind == "data_view" {
			if isAttachedSummaryState(result, holder) {
				continue
			}
			// Keep parity with legacy translator: view declarations are addressed
			// by declaration holder name (e.g. $Authorization(view/authorization)).
			inName = holder
		}
		state := &plan.State{
			Parameter: st.Parameter{
				Name: holder,
				In: &st.Location{
					Kind: st.Kind(kind),
					Name: inName,
				},
			},
		}
		if inType, outType := parseSetDeclarationTypes(block.Body); inType != "" || outType != "" {
			ensureStateSchema(state).DataType = inType
			state.OutputDataType = outType
		}
		switch st.Kind(strings.ToLower(kind)) {
		case st.KindQuery:
			required := false
			state.Required = &required
		case st.KindHeader:
			required := true
			state.Required = &required
		}
		applyDeclaredStateOptions(state, tail, rawDQL, block.BodyOffset+tailOffset, &result.Diagnostics)
		result.States = append(result.States, state)
		seen[key] = true
	}
	appendInferredPathStates(rawDQL, result, seen)
}

func appendInferredPathStates(rawDQL string, result *plan.Result, seen map[string]bool) {
	if result == nil || strings.TrimSpace(rawDQL) == "" {
		return
	}
	prepared := dqlpre.Prepare(rawDQL)
	if prepared.Directives == nil || prepared.Directives.Route == nil {
		return
	}
	for _, name := range extractRoutePathParams(prepared.Directives.Route.URI) {
		key := declaredStateKey(name, string(st.KindPath), name)
		if seen[key] {
			continue
		}
		result.States = append(result.States, &plan.State{
			Parameter: st.Parameter{
				Name: name,
				In:   st.NewPathLocation(name),
				Schema: &st.Schema{
					DataType:    "string",
					Cardinality: st.One,
				},
			},
		})
		seen[key] = true
	}
}

func extractRoutePathParams(uri string) []string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil
	}
	var result []string
	seen := map[string]bool{}
	for {
		start := strings.IndexByte(uri, '{')
		if start == -1 {
			break
		}
		uri = uri[start+1:]
		end := strings.IndexByte(uri, '}')
		if end == -1 {
			break
		}
		name := strings.TrimSpace(uri[:end])
		uri = uri[end+1:]
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, name)
	}
	return result
}

func isAttachedSummaryState(result *plan.Result, holder string) bool {
	if result == nil || strings.TrimSpace(holder) == "" {
		return false
	}
	for _, item := range result.Views {
		if item == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(item.SummaryName), strings.TrimSpace(holder)) {
			return true
		}
	}
	return false
}

func declaredStateKey(name, kind, in string) string {
	return strings.ToLower(strings.TrimSpace(name)) + "|" +
		strings.ToLower(strings.TrimSpace(kind)) + "|" +
		strings.ToLower(strings.TrimSpace(in))
}

func applyDeclaredStateOptions(state *plan.State, tail, dql string, baseOffset int, diags *[]*dqlshape.Diagnostic) {
	if state == nil || strings.TrimSpace(tail) == "" {
		return
	}
	cursor := newOptionCursor(tail)
	for cursor.next() {
		name, args := cursor.option()
		optionOffset := baseOffset + cursor.start
		switch {
		case strings.EqualFold(name, "WithURI"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.URI = trimQuote(args[0])
		case strings.EqualFold(name, "WithTag"), strings.EqualFold(name, "Tag"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.Tag = trimQuote(args[0])
		case strings.EqualFold(name, "Optional"):
			if !expectStateArgs(state, name, args, 0, 0, dql, optionOffset, diags) {
				continue
			}
			required := false
			state.Required = &required
		case strings.EqualFold(name, "Required"):
			if !expectStateArgs(state, name, args, 0, 0, dql, optionOffset, diags) {
				continue
			}
			required := true
			state.Required = &required
		case strings.EqualFold(name, "Cacheable"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			value, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				appendStateOptionDiagnostic(state, name, fmt.Sprintf("invalid bool cacheable %q", args[0]), dql, optionOffset, diags)
				continue
			}
			state.Cacheable = &value
		case strings.EqualFold(name, "QuerySelector"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.QuerySelector = trimQuote(args[0])
			if state.Cacheable == nil {
				cacheable := false
				state.Cacheable = &cacheable
			}
		case strings.EqualFold(name, "WithPredicate"), strings.EqualFold(name, "Predicate"):
			if !expectStateArgs(state, name, args, 1, -1, dql, optionOffset, diags) {
				continue
			}
			appendStatePredicate(state, args, false)
		case strings.EqualFold(name, "EnsurePredicate"):
			if !expectStateArgs(state, name, args, 1, -1, dql, optionOffset, diags) {
				continue
			}
			appendStatePredicate(state, args, true)
		case strings.EqualFold(name, "When"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.When = trimQuote(args[0])
		case strings.EqualFold(name, "Scope"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.Scope = trimQuote(args[0])
		case strings.EqualFold(name, "WithType"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			ensureStateSchema(state).DataType = trimQuote(args[0])
		case strings.EqualFold(name, "WithCodec"):
			if !expectStateArgs(state, name, args, 1, -1, dql, optionOffset, diags) {
				continue
			}
			state.Output = &st.Codec{
				Name: trimQuote(args[0]),
				Args: append([]string{}, trimQuotedArgs(args[1:])...),
			}
		case strings.EqualFold(name, "WithStatusCode"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			value, err := strconv.Atoi(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				appendStateOptionDiagnostic(state, name, fmt.Sprintf("invalid status code %q", args[0]), dql, optionOffset, diags)
				continue
			}
			state.ErrorStatusCode = value
		case strings.EqualFold(name, "WithErrorMessage"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.ErrorMessage = trimQuote(args[0])
		case strings.EqualFold(name, "Value"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			state.Value = trimQuote(args[0])
		case strings.EqualFold(name, "Embed"):
			if !expectStateArgs(state, name, args, 0, 0, dql, optionOffset, diags) {
				continue
			}
			if !strings.Contains(state.Tag, `anonymous:"true"`) {
				if strings.TrimSpace(state.Tag) != "" {
					state.Tag += " "
				}
				state.Tag += `anonymous:"true"`
			}
		case strings.EqualFold(name, "Cardinality"):
			if !expectStateArgs(state, name, args, 1, 1, dql, optionOffset, diags) {
				continue
			}
			card := strings.ToLower(strings.TrimSpace(trimQuote(args[0])))
			switch card {
			case "one":
				ensureStateSchema(state).Cardinality = st.One
			case "many":
				ensureStateSchema(state).Cardinality = st.Many
			default:
				if state != nil && state.In != nil {
					kind := strings.ToLower(state.KindString())
					if kind == "view" || kind == "data_view" {
						// Declared views already validate cardinality with DQL-VIEW-CARDINALITY.
						// Avoid duplicating that diagnostic on the shadow state projection.
						continue
					}
				}
				appendStateOptionDiagnostic(state, name, fmt.Sprintf("unsupported cardinality %q", args[0]), dql, optionOffset, diags)
			}
		case strings.EqualFold(name, "Async"):
			if !expectStateArgs(state, name, args, 0, 0, dql, optionOffset, diags) {
				continue
			}
			state.Async = true
		default:
			if state != nil && state.In != nil {
				kind := strings.ToLower(state.KindString())
				if kind == "view" || kind == "data_view" {
					// View declarations carry many view-level options (e.g. Cardinality,
					// WithURI, WithColumnType). Those are handled by declared-view parsing
					// and should not emit state-option diagnostics.
					continue
				}
			}
			appendStateOptionDiagnostic(state, name, "unknown option", dql, optionOffset, diags)
		}
	}
}

func parseSetDeclarationTypes(body string) (string, string) {
	cursor := parsly.NewCursor("", []byte(body), 0)
	if cursor.MatchAfterOptional(vdWhitespaceMatcher, vdParamDeclMatcher).Code != vdParamDeclToken {
		return "", ""
	}
	if _, matched := readIdentifier(cursor); !matched {
		return "", ""
	}
	_ = cursor.MatchOne(vdWhitespaceMatcher)
	matchedType := cursor.MatchOne(vdTypeMatcher)
	if matchedType.Code != vdTypeToken {
		return "", ""
	}
	typeExpr := strings.TrimSpace(matchedType.Text(cursor))
	if len(typeExpr) < 2 {
		return "", ""
	}
	typeExpr = strings.TrimSpace(typeExpr[1 : len(typeExpr)-1])
	if typeExpr == "" {
		return "", ""
	}
	args := splitArgs(typeExpr)
	if len(args) == 0 {
		return "", ""
	}
	inputType := strings.TrimSpace(trimQuote(args[0]))
	outputType := ""
	if len(args) > 1 {
		outputType = strings.TrimSpace(trimQuote(args[1]))
	}
	if inputType == "?" {
		inputType = ""
	}
	if outputType == "?" {
		outputType = ""
	}
	return inputType, outputType
}

func trimQuotedArgs(input []string) []string {
	if len(input) == 0 {
		return nil
	}
	result := make([]string, 0, len(input))
	for _, item := range input {
		result = append(result, trimQuote(item))
	}
	return result
}

func appendStatePredicate(state *plan.State, args []string, ensure bool) {
	if state == nil || len(args) == 0 {
		return
	}
	group := 0
	nameIdx := 0
	if len(args) >= 2 {
		if parsed, err := strconv.Atoi(strings.TrimSpace(trimQuote(args[0]))); err == nil {
			group = parsed
			nameIdx = 1
		}
	}
	if len(args) <= nameIdx {
		return
	}
	predicate := &extension.PredicateConfig{
		Group:  group,
		Name:   trimQuote(args[nameIdx]),
		Ensure: ensure,
		Args:   []string{},
	}
	for _, arg := range args[nameIdx+1:] {
		predicate.Args = append(predicate.Args, trimQuote(arg))
	}
	state.Predicates = append(state.Predicates, predicate)
}

func ensureStateSchema(state *plan.State) *st.Schema {
	if state.Schema == nil {
		state.Schema = &st.Schema{}
	}
	return state.Schema
}

type optionCursor struct {
	raw    string
	cursor int
	start  int
	name   string
	args   []string
}

func newOptionCursor(raw string) *optionCursor {
	return &optionCursor{raw: raw}
}

func (o *optionCursor) next() bool {
	o.name = ""
	o.args = nil
	o.start = 0
	for o.cursor < len(o.raw) && (o.raw[o.cursor] == ' ' || o.raw[o.cursor] == '\n' || o.raw[o.cursor] == '\t' || o.raw[o.cursor] == '\r') {
		o.cursor++
	}
	if o.cursor >= len(o.raw) || o.raw[o.cursor] != '.' {
		return false
	}
	o.start = o.cursor
	o.cursor++
	start := o.cursor
	for o.cursor < len(o.raw) {
		ch := o.raw[o.cursor]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			o.cursor++
			continue
		}
		break
	}
	if o.cursor == start {
		return false
	}
	o.name = strings.TrimSpace(o.raw[start:o.cursor])
	for o.cursor < len(o.raw) && (o.raw[o.cursor] == ' ' || o.raw[o.cursor] == '\n' || o.raw[o.cursor] == '\t' || o.raw[o.cursor] == '\r') {
		o.cursor++
	}
	if o.cursor >= len(o.raw) || o.raw[o.cursor] != '(' {
		return false
	}
	groupStart := o.cursor
	depth := 0
	inSingle := false
	inDouble := false
	escape := false
	for o.cursor < len(o.raw) {
		ch := o.raw[o.cursor]
		if escape {
			escape = false
			o.cursor++
			continue
		}
		switch ch {
		case '\\':
			escape = true
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 {
					o.cursor++
					content := o.raw[groupStart+1 : o.cursor-1]
					o.args = splitArgs(content)
					return true
				}
			}
		}
		o.cursor++
	}
	return false
}

func (o *optionCursor) option() (string, []string) {
	return o.name, o.args
}

func expectStateArgs(state *plan.State, option string, args []string, min, max int, dql string, offset int, diags *[]*dqlshape.Diagnostic) bool {
	if len(args) < min {
		appendStateOptionDiagnostic(state, option, fmt.Sprintf("expected at least %d args, got %d", min, len(args)), dql, offset, diags)
		return false
	}
	if max >= 0 && len(args) > max {
		appendStateOptionDiagnostic(state, option, fmt.Sprintf("expected at most %d args, got %d", max, len(args)), dql, offset, diags)
		return false
	}
	return true
}

func appendStateOptionDiagnostic(state *plan.State, option, detail, dql string, offset int, diags *[]*dqlshape.Diagnostic) {
	stateName := ""
	if state != nil {
		stateName = state.Name
	}
	*diags = append(*diags, &dqlshape.Diagnostic{
		Code:     dqldiag.CodeDeclOptionArgs,
		Severity: dqlshape.SeverityWarning,
		Message:  fmt.Sprintf("invalid %s declaration for state %q: %s", option, stateName, detail),
		Hint:     "check option name, arity and argument formatting",
		Span:     relationSpan(dql, offset),
	})
}
