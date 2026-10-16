// Package identifier renders immutable application constants at SQL identifier
// sites using native Velty spans and SQLParser protected regions. It never
// accepts invocation values or changes authored SQL.
package identifier

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// Reference identifies an immutable constant site in the original source.
type Reference struct {
	Start, End int
	Name       string
	Value      string
}

// Renderer uses Velty's emitted-expression spans and SQLParser's quote
// boundaries. Template decisions, calls, locals and SQL string/comment data are
// not constant identifier sites. A qualified emitted reference rooted in a
// declared scalar constant is an identifier; ordinary scalar emission stays bound.
type Renderer struct {
	Values    *constant.Values
	Variables []string
	Product   string
	Connector string
	// Products resolves source authority without querying a database.
	Products func(connector string) (string, error)
}

func (r Renderer) parts(value string) ([]string, error) {
	product := r.Product
	if r.Products != nil {
		var err error
		product, err = r.Products(r.Connector)
		if err != nil {
			return nil, fmt.Errorf("connector %q identifier product: %w", r.Connector, err)
		}
	}
	return (sqlparser.TableIdentifierParser{Product: product}).Parts(value)
}

// ForView preserves inherited connector authority unless the view overrides it.
func (r Renderer) ForView(view *spec.View) Renderer {
	if view != nil && view.Source != nil && view.Source.Bindings != nil {
		if connector := strings.TrimSpace(view.Source.Bindings.Connector); connector != "" {
			r.Connector = connector
		}
	}
	return r
}

func (r Renderer) References(source string) ([]Reference, error) {
	if r.Values == nil || !strings.Contains(source, "$") {
		return nil, nil
	}
	root, spans, err := veltyparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return nil, fmt.Errorf("parse constant SQL references: %w", err)
	}
	locals := map[string]bool{}
	CollectLocalVariables(root.Statements(), locals)
	var replacements []Reference
	var visit func([]ast.Statement) error
	visit = func(statements []ast.Statement) error {
		for _, statement := range statements {
			switch node := statement.(type) {
			case *expr.Select:
				value, found := r.Values.Lookup(node.ID)
				if locals[node.ID] {
					continue
				}
				span, ok := spans[node]
				if !ok || span.Start < 0 || span.End >= len(source) {
					return fmt.Errorf("constant %q has no source span", node.ID)
				}
				start, end, kind := sqltext.ProtectedRangeAt(source, span.Start)
				quoted := kind != "" && (source[start] == '`' || source[start] == '[')
				if kind != "" && !quoted {
					continue
				}
				pure := true
				for next := node.X; next != nil; {
					part, ok := next.(*expr.Select)
					if !ok {
						pure = false
						break
					}
					next = part.X
				}
				if !pure {
					continue
				}
				prefix := span.Start + 1
				if prefix < len(source) && source[prefix] == '!' {
					continue
				}
				braced := prefix < len(source) && source[prefix] == '{'
				if braced {
					prefix++
				}
				prefix += len(node.ID)
				if braced {
					if prefix >= len(source) || source[prefix] != '}' {
						continue
					}
					prefix++
				}
				qualified := prefix < len(source) && source[prefix] == '.'
				fragment := span.Start > 0 && identifierByte(source[span.Start-1])
				if !quoted && !qualified && !fragment {
					continue
				}
				if quoted && prefix >= end {
					continue
				}
				if !found {
					known := false
					for _, name := range append([]string{"Unsafe", "View", "predicate", "criteria", "SQLBindings"}, r.Variables...) {
						if strings.EqualFold(name, node.ID) {
							known = true
							break
						}
					}
					if known {
						continue
					}
					return fmt.Errorf("unknown identifier constant %q", node.ID)
				}
				if value == "" {
					// An empty suffix cannot supply an entire identifier. An
					// unquoted fragment already has a literal identifier prefix.
					if !quoted && !fragment {
						return fmt.Errorf("identifier constant %q is empty", node.ID)
					}
					if quoted {
						remainder := source[start+1:span.Start] + source[prefix:end-1]
						parts, err := r.parts(remainder)
						if err != nil || len(parts) != 1 || strings.ContainsAny(remainder, "$.") {
							return fmt.Errorf("empty identifier constant %q leaves an incomplete identifier", node.ID)
						}
					}
				}
				// Identifier fragments cannot introduce SQL syntax or an enclosing quote.
				// Hyphens are useful for BigQuery project IDs; authors own SQL delimiting.
				for _, ch := range value {
					if !(ch == '_' || ch == '-' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9') {
						return fmt.Errorf("identifier constant %q must contain only letters, digits, underscores or hyphens", node.ID)
					}
				}
				replacements = append(replacements, Reference{Start: span.Start, End: prefix, Name: node.ID, Value: value})
			case *stmt.If:
				if err := visit(node.Body.Statements()); err != nil {
					return err
				}
				for branch := node.Else; branch != nil; branch = branch.Else {
					if err := visit(branch.Body.Statements()); err != nil {
						return err
					}
				}
			case *stmt.ForEach:
				if err := visit(node.Statements()); err != nil {
					return err
				}
			case *stmt.ForLoop:
				if err := visit(node.Statements()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(root.Statements()); err != nil {
		return nil, err
	}
	return replacements, nil
}

func identifierByte(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_'
}

func (r Renderer) Render(source string) (string, error) {
	replacements, err := r.References(source)
	if err != nil {
		return "", err
	}
	sort.Slice(replacements, func(i, j int) bool { return replacements[i].Start > replacements[j].Start })
	result := source
	for _, replacement := range replacements {
		result = result[:replacement.Start] + replacement.Value + result[replacement.End:]
	}
	return result, nil
}

// Validate checks identifier values and native source spans without expanding SQL.
func (r Renderer) Validate(source string) error { _, err := r.References(source); return err }

// Identifier renders transient table metadata through the SQL rendering owner.
func (r Renderer) Identifier(table string) (string, error) {
	if name := constant.TableName(table); name != "" {
		if value, found := r.Values.Lookup(name); found {
			if _, err := r.parts(value); err != nil {
				return "", fmt.Errorf("identifier constant %q: %w", name, err)
			}
			return value, nil
		}
	}
	const prefix = "SELECT * FROM "
	rendered, err := r.Render(prefix + table)
	if err != nil {
		return "", err
	}
	return rendered[len(prefix):], nil
}

// CollectLocalVariables retains the existing native template local-variable boundary.
func CollectLocalVariables(statements []ast.Statement, result map[string]bool) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *stmt.Statement:
			collectAssignedVariable(actual, result)
		case *stmt.If:
			CollectLocalVariables(actual.Body.Statements(), result)
			for branch := actual.Else; branch != nil; branch = branch.Else {
				CollectLocalVariables(branch.Body.Statements(), result)
			}
		case *stmt.ForEach:
			if actual.Item != nil {
				result[actual.Item.ID] = true
			}
			if actual.Index != nil {
				result[actual.Index.ID] = true
			}
			CollectLocalVariables(actual.Statements(), result)
		case *stmt.ForLoop:
			if assignment, ok := actual.Init.(*stmt.Statement); ok {
				collectAssignedVariable(assignment, result)
			}
			CollectLocalVariables(actual.Statements(), result)
		}
	}
}

func collectAssignedVariable(assignment *stmt.Statement, result map[string]bool) {
	if assignment == nil {
		return
	}
	if target, ok := assignment.X.(*expr.Select); ok && target.ID != "" {
		result[target.ID] = true
	}
}
