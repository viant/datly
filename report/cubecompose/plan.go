package cubecompose

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

const sourcePrefix = "DATLYCUBESQL"

type Role string

const (
	Dimension Role = "dimension"
	Measure   Role = "measure"
	Computed  Role = "expression"
)

type Field struct {
	Name string
	// SQLName is the declared source output. Name may differ only for an
	// explicitly authored mapping established by the report source catalog.
	SQLName string
	Type    reflect.Type
	Role    Role
}

type Catalog struct {
	fields map[string]Field
}

type Column struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Role  Role   `json:"role"`
	rType reflect.Type
}

type Plan struct {
	selectQuery *query.Select
	catalog     *Catalog
	nullable    []bool
	Columns     []Column
	Fields      [][]string
	FrameCount  int
	Limit       int
}

// Frame is one trusted, parameterized cube projection supplied to Render.
type Frame struct {
	SQL  string
	Args []interface{}
}

type renderer struct {
	catalog *Catalog
	aliases map[string]Column
	args    []interface{}
}

var safeIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func NewCatalog(fields ...Field) (*Catalog, error) {
	result := &Catalog{fields: map[string]Field{}}
	for _, field := range fields {
		if !safeIdentifier.MatchString(field.Name) {
			return nil, fmt.Errorf("cube compose field %q is not a safe SQL identifier", field.Name)
		}
		if field.SQLName == "" {
			field.SQLName = field.Name
		}
		if !safeIdentifier.MatchString(field.SQLName) {
			return nil, fmt.Errorf("cube compose source column %q is not a safe SQL identifier", field.SQLName)
		}
		if field.Role != Dimension && field.Role != Measure {
			return nil, fmt.Errorf("cube compose field %q has unsupported role %q", field.Name, field.Role)
		}
		key := normalize(field.Name)
		if _, ok := result.fields[key]; ok {
			return nil, fmt.Errorf("duplicate cube compose field %q", field.Name)
		}
		result.fields[key] = field
	}
	return result, nil
}

func (c *Catalog) Lookup(name string) (Field, bool) {
	if c == nil {
		return Field{}, false
	}
	field, ok := c.fields[normalize(name)]
	return field, ok
}

func Compile(SQL string, catalog *Catalog, frameCount, maxLimit int) (*Plan, error) {
	if catalog == nil || len(catalog.fields) == 0 {
		return nil, fmt.Errorf("cube compose field catalog was empty")
	}
	if frameCount <= 0 {
		return nil, fmt.Errorf("cube compose requires at least one cube")
	}
	if maxLimit <= 0 {
		maxLimit = 100
	}
	if strings.TrimSpace(SQL) == "" {
		return nil, fmt.Errorf("cube compose SQL was empty")
	}
	normalizedSQL, err := replaceCubeMacros(SQL, frameCount)
	if err != nil {
		return nil, err
	}
	parsed, err := sqlparser.ParseQuery(normalizedSQL)
	if err != nil {
		return nil, fmt.Errorf("invalid cube compose SQL: %w", err)
	}
	plan := &Plan{selectQuery: parsed, catalog: catalog, FrameCount: frameCount}
	if err := plan.validate(maxLimit); err != nil {
		return nil, err
	}
	return plan, nil
}

func (p *Plan) validate(maxLimit int) error {
	q := p.selectQuery
	if q == nil {
		return fmt.Errorf("cube compose query was empty")
	}
	if len(q.WithSelects) != 0 || q.WithRecursive {
		return fmt.Errorf("cube compose v1 does not allow WITH clauses")
	}
	if q.Union != nil {
		return fmt.Errorf("cube compose v1 does not allow UNION clauses")
	}
	if q.Offset != nil {
		return fmt.Errorf("cube compose v1 does not allow OFFSET clauses")
	}
	// sqlparser records LIMIT/OFFSET's keyword in Window as well as populating
	// Limit/Offset. LIMIT is the only supported windowing clause here.
	if q.Window != nil && !strings.EqualFold(strings.TrimSpace(q.Window.Raw), "limit") {
		return fmt.Errorf("cube compose v1 does not allow WINDOW clauses")
	}
	if q.Kind != "" {
		return fmt.Errorf("cube compose selection modifier %q is not allowed", q.Kind)
	}
	if sourceFrame(q.From.X, p.FrameCount) != 1 || !strings.EqualFold(q.From.Alias, "t1") {
		return fmt.Errorf("cube compose FROM must be %s AS t1", cubeMacro(1))
	}
	if q.From.Comments != "" || q.From.Unparsed != "" || len(q.Joins) != p.FrameCount-1 {
		return fmt.Errorf("cube compose requires exactly %d explicit joins for %d cubes", p.FrameCount-1, p.FrameCount)
	}

	aliases := map[string]Column{}
	p.nullable = make([]bool, p.FrameCount)
	seen := make([]map[string]bool, p.FrameCount)
	for i := range seen {
		seen[i] = map[string]bool{}
	}
	for i, join := range q.Joins {
		frame := i + 2
		alias := cubeAlias(frame)
		if sourceFrame(join.With, p.FrameCount) != frame || !strings.EqualFold(join.Alias, alias) {
			return fmt.Errorf("cube compose join %d must target %s AS %s", i+1, cubeMacro(frame), alias)
		}
		if join.Comments != "" || join.On == nil || join.On.X == nil {
			return fmt.Errorf("cube compose join %d requires an ON expression", i+1)
		}
		joinKind, err := normalizeJoin(join.Raw)
		if err != nil {
			return err
		}
		p.nullable[frame-1] = joinKind == "LEFT JOIN"
		joinCount, err := p.validateJoin(join.On.X, frame)
		if err != nil {
			return err
		}
		if joinCount == 0 {
			return fmt.Errorf("cube compose join %d requires at least one matching dimension key", i+1)
		}
		joinRefs, err := p.validateNode(join.On.X, nil)
		if err != nil {
			return err
		}
		appendRefs(joinRefs, seen)
	}
	for _, item := range q.List {
		if item == nil || item.Expr == nil {
			return fmt.Errorf("cube compose projection contains an empty expression")
		}
		refs, err := p.validateNode(item.Expr, nil)
		if err != nil {
			return fmt.Errorf("invalid cube compose projection: %w", err)
		}
		appendRefs(refs, seen)
		name, role, rType, err := p.outputColumn(item)
		if err != nil {
			return err
		}
		key := normalize(name)
		if _, ok := aliases[key]; ok {
			return fmt.Errorf("duplicate cube compose output name %q", name)
		}
		column := Column{Name: name, Type: typeName(rType), Role: role, rType: rType}
		aliases[key] = column
		p.Columns = append(p.Columns, column)
	}
	if len(p.Columns) == 0 {
		return fmt.Errorf("cube compose requires at least one projected field")
	}

	// Projection aliases are not portable in WHERE, but are supported in
	// HAVING and ORDER BY by the target warehouse dialects.
	refs, err := p.validateNode(qualifyNode(q.Qualify), nil)
	if err != nil {
		return err
	}
	appendRefs(refs, seen)
	refs, err = p.validateNode(qualifyNode(q.Having), aliases)
	if err != nil {
		return err
	}
	appendRefs(refs, seen)
	for _, list := range []query.List{q.GroupBy, q.OrderBy} {
		for _, item := range list {
			if item == nil {
				continue
			}
			if item.Direction != "" && !strings.EqualFold(item.Direction, "ASC") && !strings.EqualFold(item.Direction, "DESC") {
				return fmt.Errorf("unsupported cube compose sort direction %q", item.Direction)
			}
			refs, err := p.validateNode(item.Expr, aliases)
			if err != nil {
				return err
			}
			appendRefs(refs, seen)
		}
	}
	p.Fields = make([][]string, p.FrameCount)
	for i := range seen {
		p.Fields[i] = sortedFields(seen[i])
	}

	if q.Limit == nil {
		p.Limit = maxLimit
		q.Limit = expr.NewIntLiteral(strconv.Itoa(maxLimit))
	} else {
		limit, err := strconv.Atoi(strings.TrimSpace(q.Limit.Value))
		if err != nil || limit <= 0 || limit > maxLimit {
			return fmt.Errorf("cube compose LIMIT must be between 1 and %d", maxLimit)
		}
		p.Limit = limit
	}
	return nil
}

// RuntimeType returns the projected scalar type, or nil for a computed expression.
func (c Column) RuntimeType() reflect.Type { return c.rType }

type fieldRef struct {
	alias string
	field Field
}

func (p *Plan) validateJoin(n node.Node, frame int) (int, error) {
	terms, err := joinTerms(n)
	if err != nil {
		return 0, err
	}
	for _, term := range terms {
		if err := p.validateJoinTerm(term, frame); err != nil {
			return 0, err
		}
	}
	return len(terms), nil
}

func joinTerms(n node.Node) ([]*expr.Binary, error) {
	n = unwrapParenthesis(n)
	binary, ok := n.(*expr.Binary)
	if !ok {
		return nil, fmt.Errorf("cube compose ON supports dimension equality joined with AND")
	}
	if strings.EqualFold(strings.TrimSpace(binary.Op), "AND") {
		left, err := joinTerms(binary.X)
		if err != nil {
			return nil, err
		}
		right, err := joinTerms(binary.Y)
		return append(left, right...), err
	}
	if strings.TrimSpace(binary.Op) != "=" {
		return nil, fmt.Errorf("cube compose join keys must use equality")
	}
	return []*expr.Binary{binary}, nil
}

func (p *Plan) validateJoinTerm(binary *expr.Binary, frame int) error {
	left, err := p.resolveSelector(binary.X)
	if err != nil {
		return err
	}
	right, err := p.resolveSelector(binary.Y)
	if err != nil {
		return err
	}
	if left.field.Role != Dimension || right.field.Role != Dimension {
		return fmt.Errorf("cube compose join keys must be dimensions")
	}
	leftFrame, _ := aliasFrame(left.alias, p.FrameCount)
	rightFrame, _ := aliasFrame(right.alias, p.FrameCount)
	if leftFrame == rightFrame || !joinsNewFrame(leftFrame, rightFrame, frame) {
		return fmt.Errorf("cube compose join for %s must match one of its dimensions to an earlier cube", cubeAlias(frame))
	}
	if left.field.SQLName != right.field.SQLName {
		return fmt.Errorf("cube compose join dimensions must match, got %s and %s", left.field.Name, right.field.Name)
	}
	return nil
}

func (p *Plan) outputColumn(item *query.Item) (string, Role, reflect.Type, error) {
	if ref, err := p.resolveSelector(item.Expr); err == nil {
		name := ref.field.Name
		if item.Alias != "" {
			name = item.Alias
		}
		if !safeIdentifier.MatchString(name) {
			return "", "", nil, fmt.Errorf("invalid cube compose output name %q", name)
		}
		rType := ref.field.Type
		if frame, ok := aliasFrame(ref.alias, p.FrameCount); ok && p.nullable[frame-1] && rType != nil && rType.Kind() != reflect.Ptr {
			rType = reflect.PtrTo(rType)
		}
		return name, ref.field.Role, rType, nil
	}
	if item.Alias == "" || !safeIdentifier.MatchString(item.Alias) {
		return "", "", nil, fmt.Errorf("computed cube compose projections require a safe explicit alias")
	}
	return item.Alias, Computed, nil, nil
}

func (p *Plan) validateNode(n node.Node, aliases map[string]Column) ([]fieldRef, error) {
	if n == nil {
		return nil, nil
	}
	switch actual := n.(type) {
	case []node.Node:
		if len(actual) == 0 {
			return nil, fmt.Errorf("empty cube compose expression list is not allowed")
		}
		var refs []fieldRef
		for _, item := range actual {
			itemRefs, err := p.validateNode(item, aliases)
			if err != nil {
				return nil, err
			}
			refs = append(refs, itemRefs...)
		}
		return refs, nil
	case *expr.Selector:
		ref, err := p.resolveSelector(actual)
		if err != nil {
			return nil, err
		}
		return []fieldRef{ref}, nil
	case *expr.Ident:
		if aliases != nil {
			if _, ok := aliases[normalize(actual.Name)]; ok {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("unqualified field %q is not allowed", actual.Name)
	case *expr.Literal:
		return nil, validateLiteral(actual)
	case *expr.Placeholder:
		return nil, fmt.Errorf("caller placeholders are not allowed")
	case *expr.Star:
		return nil, fmt.Errorf("wildcard projection is not allowed")
	case *expr.Binary:
		if !allowedOperator(actual.Op) {
			return nil, fmt.Errorf("unsupported cube compose operator %q", actual.Op)
		}
		left, err := p.validateNode(actual.X, aliases)
		if err != nil {
			return nil, err
		}
		right, err := p.validateNode(actual.Y, aliases)
		return append(left, right...), err
	case *expr.Unary:
		if op := strings.ToUpper(strings.TrimSpace(actual.Op)); op != "NOT" && op != "-" && op != "+" && op != "IS NULL" && op != "IS NOT NULL" {
			return nil, fmt.Errorf("unsupported cube compose unary operator %q", actual.Op)
		}
		return p.validateNode(actual.X, aliases)
	case *expr.Parenthesis:
		if actual.X == nil {
			return nil, fmt.Errorf("opaque parenthesized expression is not allowed")
		}
		return p.validateNode(actual.X, aliases)
	case *expr.Qualify:
		return p.validateNode(actual.X, aliases)
	case *expr.Range:
		left, err := p.validateNode(actual.Min, aliases)
		if err != nil {
			return nil, err
		}
		right, err := p.validateNode(actual.Max, aliases)
		return append(left, right...), err
	case *expr.Call:
		name := sourceName(actual.X)
		if !allowedFunction(name) {
			return nil, fmt.Errorf("unsupported cube compose function %q", name)
		}
		var refs []fieldRef
		for _, arg := range actual.Args {
			items, err := p.validateNode(arg, aliases)
			if err != nil {
				return nil, err
			}
			refs = append(refs, items...)
		}
		return refs, nil
	case *expr.Switch, *expr.Raw:
		return nil, fmt.Errorf("opaque SQL expression %T is not allowed", n)
	default:
		return nil, fmt.Errorf("unsupported cube compose expression %T", n)
	}
}

func unwrapParenthesis(n node.Node) node.Node {
	for {
		parenthesis, ok := n.(*expr.Parenthesis)
		if !ok || parenthesis.X == nil {
			return n
		}
		n = parenthesis.X
	}
}

func (p *Plan) resolveSelector(n node.Node) (fieldRef, error) {
	selector, ok := n.(*expr.Selector)
	if !ok {
		return fieldRef{}, fmt.Errorf("expected qualified cube field, got %T", n)
	}
	parts := selectorParts(selector)
	if len(parts) != 2 {
		return fieldRef{}, fmt.Errorf("cube fields must be qualified as tN.<field>")
	}
	if _, ok := aliasFrame(parts[0], p.FrameCount); !ok {
		return fieldRef{}, fmt.Errorf("cube alias %q does not identify one of the %d submitted cubes", parts[0], p.FrameCount)
	}
	field, ok := p.catalog.Lookup(parts[1])
	if !ok {
		return fieldRef{}, fmt.Errorf("field %q is not an exposed cube dimension or measure", parts[1])
	}
	return fieldRef{alias: parts[0], field: field}, nil
}

func selectorParts(selector *expr.Selector) []string {
	parts := []string{normalize(selector.Name)}
	next := selector.X
	for next != nil {
		switch actual := next.(type) {
		case *expr.Ident:
			parts = append(parts, normalize(actual.Name))
			next = nil
		case *expr.Selector:
			parts = append(parts, normalize(actual.Name))
			next = actual.X
		default:
			return nil
		}
	}
	return parts
}

func sourceName(n node.Node) string {
	switch actual := n.(type) {
	case *expr.Ident:
		return actual.Name
	case *expr.Selector:
		parts := selectorParts(actual)
		return strings.Join(parts, ".")
	}
	return ""
}

func sourceFrame(n node.Node, frameCount int) int {
	name := sourceName(n)
	for frame := 1; frame <= frameCount; frame++ {
		if name == cubeSource(frame) {
			return frame
		}
	}
	return 0
}

func aliasFrame(alias string, frameCount int) (int, bool) {
	alias = normalize(alias)
	if len(alias) < 2 || alias[0] != 't' {
		return 0, false
	}
	frame, err := strconv.Atoi(alias[1:])
	if err != nil || frame < 1 || frame > frameCount || cubeAlias(frame) != alias {
		return 0, false
	}
	return frame, true
}

func joinsNewFrame(left, right, frame int) bool {
	return left == frame && right > 0 && right < frame || right == frame && left > 0 && left < frame
}

func normalizeJoin(raw string) (string, error) {
	value := strings.ToUpper(strings.Join(strings.Fields(raw), " "))
	switch value {
	case "JOIN", "INNER JOIN":
		return "JOIN", nil
	case "LEFT JOIN", "LEFT OUTER JOIN":
		return "LEFT JOIN", nil
	default:
		return "", fmt.Errorf("unsupported cube compose join %q: use JOIN or LEFT JOIN and swap cube frames when needed", raw)
	}
}

func qualifyNode(value *expr.Qualify) node.Node {
	if value == nil {
		return nil
	}
	return value.X
}

func appendRefs(refs []fieldRef, fields []map[string]bool) {
	for _, ref := range refs {
		frame, ok := aliasFrame(ref.alias, len(fields))
		if ok {
			fields[frame-1][ref.field.Name] = true
		}
	}
}

func sortedFields(input map[string]bool) []string {
	result := make([]string, 0, len(input))
	for value := range input {
		result = append(result, value)
	}
	sortStrings(result)
	return result
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(strings.Trim(value, "`")))
}

func typeName(rType reflect.Type) string {
	if rType == nil {
		return "any"
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType.String()
}

func allowedOperator(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "=", "!=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "AND", "OR", "IN", "NOT IN", "LIKE", "BETWEEN", "IS", "IS NOT":
		return true
	}
	return false
}

func allowedFunction(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ABS", "COALESCE", "NULLIF", "ROUND", "SUM", "MIN", "MAX", "AVG", "COUNT":
		return true
	}
	return false
}

func validateLiteral(literal *expr.Literal) error {
	_, err := literalValue(literal)
	return err
}

func literalValue(literal *expr.Literal) (interface{}, error) {
	value := strings.TrimSpace(literal.Value)
	switch strings.ToLower(literal.Kind) {
	case "int":
		return strconv.ParseInt(value, 10, 64)
	case "numeric":
		return strconv.ParseFloat(value, 64)
	case "bool":
		return strconv.ParseBool(strings.ToLower(value))
	case "null":
		return nil, nil
	case "string":
		if len(value) < 2 || (value[0] != '\'' && value[0] != '"') || value[len(value)-1] != value[0] {
			return nil, fmt.Errorf("invalid SQL string literal")
		}
		inner := value[1 : len(value)-1]
		if value[0] == '\'' {
			inner = strings.ReplaceAll(inner, "''", "'")
		}
		return inner, nil
	default:
		return nil, fmt.Errorf("unsupported SQL literal kind %q", literal.Kind)
	}
}
