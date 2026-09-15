package template

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/constant"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// constantBindings contains only validated, trusted values. Native Velty emits
// them during SQL evaluation; authored and compiled SQL never contain the values.
type constantBindings struct{ values []string }

func (c *constantBindings) Get(index int) (string, error) {
	if c == nil || index < 0 || index >= len(c.values) {
		return "", fmt.Errorf("invalid SQL constant slot")
	}
	return c.values[index], nil
}

// ConstantRenderer uses Velty's emitted-expression spans and SQLParser's quote
// boundaries. Template decisions, calls, locals and SQL string/comment data are
// not constant identifier sites. A qualified emitted reference rooted in a
// declared scalar constant is an identifier; ordinary scalar emission stays bound.
type ConstantRenderer struct {
	Values    *constant.Values
	Variables []string
}

func (r ConstantRenderer) plan(source string) ([]sourceReplacement, error) {
	if r.Values == nil || !strings.Contains(source, "$") {
		return nil, nil
	}
	root, spans, err := veltyparser.ParseWithSpansDetailed([]byte(source))
	if err != nil {
		return nil, fmt.Errorf("parse constant SQL references: %w", err)
	}
	locals := map[string]bool{}
	collectLocalVariables(root.Statements(), locals)
	var replacements []sourceReplacement
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
				if !quoted && !qualified {
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
					return fmt.Errorf("identifier constant %q is empty", node.ID)
				}
				// Identifier fragments cannot introduce SQL syntax or an enclosing quote.
				// Hyphens are useful for BigQuery project IDs; authors own SQL delimiting.
				for _, ch := range value {
					if !(ch == '_' || ch == '-' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9') {
						return fmt.Errorf("identifier constant %q must contain only letters, digits, underscores or hyphens", node.ID)
					}
				}
				replacements = append(replacements, sourceReplacement{start: span.Start, end: prefix, value: value})
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

func (r ConstantRenderer) render(source string) (string, error) {
	replacements, err := r.plan(source)
	if err != nil {
		return "", err
	}
	sort.Slice(replacements, func(i, j int) bool { return replacements[i].start > replacements[j].start })
	result := source
	for _, replacement := range replacements {
		result = result[:replacement.start] + replacement.value + result[replacement.end:]
	}
	return result, nil
}

// Validate checks identifier values and native source spans without expanding SQL.
func (r ConstantRenderer) Validate(source string) error { _, err := r.plan(source); return err }

// Identifier renders transient table metadata through the SQL rendering owner.
func (r ConstantRenderer) Identifier(table string) (string, error) {
	const prefix = "SELECT * FROM "
	rendered, err := r.render(prefix + table)
	if err != nil {
		return "", err
	}
	return rendered[len(prefix):], nil
}

func (c Compiler) constantRenderer() ConstantRenderer {
	names := append([]string(nil), c.NonWindowAliases...)
	for _, variable := range c.Variables {
		names = append(names, variable.Name)
	}
	return ConstantRenderer{Values: c.Const, Variables: names}
}

// constantSource compiles reference locations into native template slots. It
// changes only a private compilation copy, and never substitutes actual values.
func (c Compiler) constantSource() (string, *constantBindings, error) {
	references, err := c.constantRenderer().plan(c.Source)
	if err != nil {
		return "", nil, err
	}
	if len(references) == 0 {
		return c.Source, nil, nil
	}
	sort.Slice(references, func(i, j int) bool { return references[i].start > references[j].start })
	bindings := &constantBindings{values: make([]string, len(references))}
	source := c.Source
	for index, reference := range references {
		bindings.values[index] = reference.value
		slot := fmt.Sprintf("${%s.Get(%d)}", constantVariable, index)
		source = source[:reference.start] + slot + source[reference.end:]
	}
	return source, bindings, nil
}
