package template

import (
	"fmt"
	"github.com/viant/datly/constant/identifier"
	"reflect"
	"sort"

	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

type sourceReplacement struct {
	start int
	end   int
	value string
}

// bindEmittedInputVariables mirrors Datly's SQL sanitization boundary: input
// values used as template decisions stay typed, while values emitted into SQL
// become named placeholders consumed by the shared SQL binder.
func bindEmittedInputVariables(source string, root *stmt.Block, spans map[ast.Node]veltyparser.NodeSpan, bindings []variableBinding, nullableTypes *[]reflect.Type) (string, error) {
	byName := make(map[string]variableBinding, len(bindings))
	for _, binding := range bindings {
		byName[binding.name] = binding
	}
	locals := map[string]bool{}
	identifier.CollectLocalVariables(root.Statements(), locals)
	var replacements []sourceReplacement
	if err := collectInputReplacements(source, root.Statements(), spans, byName, locals, &replacements, nullableTypes); err != nil {
		return "", err
	}
	if len(replacements) == 0 {
		return source, nil
	}
	sort.Slice(replacements, func(i, j int) bool {
		return replacements[i].start > replacements[j].start
	})
	result := source
	for _, replacement := range replacements {
		if replacement.start < 0 || replacement.end < replacement.start || replacement.end > len(result) {
			return "", fmt.Errorf("bind emitted SQL template variable: invalid source span [%d:%d]", replacement.start, replacement.end)
		}
		result = result[:replacement.start] + replacement.value + result[replacement.end:]
	}
	return result, nil
}

func collectInputReplacements(source string, statements []ast.Statement, spans map[ast.Node]veltyparser.NodeSpan, bindings map[string]variableBinding, locals map[string]bool, replacements *[]sourceReplacement, nullableTypes *[]reflect.Type) error {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *expr.Select:
			binding, ok := bindings[actual.ID]
			isLocal := locals[actual.ID]
			if !ok && !isLocal {
				continue
			}
			span, ok := spans[actual]
			if !ok {
				return fmt.Errorf("bind emitted SQL template variable $%s: source span is unavailable", actual.ID)
			}
			end := span.End + 1
			if span.Start < 0 || end > len(source) || end <= span.Start {
				return fmt.Errorf("bind emitted SQL template variable $%s: invalid source span [%d:%d]", actual.ID, span.Start, end)
			}
			if protected := sqltext.ProtectionAt(source, span.Start); protected != "" {
				return fmt.Errorf("SQL template variable $%s cannot be emitted inside %s; remove SQL quoting/commenting around the value", actual.ID, protected)
			}
			replacement := ":" + binding.placeholderName
			if binding.placeholderName == "" || isLocal || actual.X != nil {
				replacement = fmt.Sprintf("${%s.Add(%s)}", bindingVariable, source[span.Start:end])
				if !isLocal {
					if memberType := nullableMemberType(binding.valueType, actual.X); memberType != nil {
						index := len(*nullableTypes)
						*nullableTypes = append(*nullableTypes, memberType)
						replacement = fmt.Sprintf("${%s.AddNullable(%s, %d)}", bindingVariable, source[span.Start:end], index)
					}
				}
			}
			*replacements = append(*replacements, sourceReplacement{
				start: span.Start,
				end:   end,
				value: replacement,
			})
		case *stmt.If:
			if err := collectInputReplacements(source, actual.Body.Statements(), spans, bindings, locals, replacements, nullableTypes); err != nil {
				return err
			}
			for branch := actual.Else; branch != nil; branch = branch.Else {
				if err := collectInputReplacements(source, branch.Body.Statements(), spans, bindings, locals, replacements, nullableTypes); err != nil {
					return err
				}
			}
		case *stmt.ForEach:
			if err := collectInputReplacements(source, actual.Statements(), spans, bindings, locals, replacements, nullableTypes); err != nil {
				return err
			}
		case *stmt.ForLoop:
			if err := collectInputReplacements(source, actual.Statements(), spans, bindings, locals, replacements, nullableTypes); err != nil {
				return err
			}
		}
	}
	return nil
}

// Only declared plain exported members can seed a nullable binding. Calls,
// dynamic indexing, interfaces and inaccessible fields keep their existing
// compiler behavior; this is not a value resolver or a new visibility path.
func nullableMemberType(root reflect.Type, expression ast.Expression) reflect.Type {
	if root == nil || expression == nil {
		return nil
	}
	for expression != nil {
		member, ok := expression.(*expr.Select)
		if !ok {
			return nil
		}
		root = dereferenceType(root)
		if root == nil || root.Kind() != reflect.Struct {
			return nil
		}
		field, found := root.FieldByName(member.ID)
		if !found || field.PkgPath != "" {
			return nil
		}
		root, expression = field.Type, member.X
	}
	if root.Kind() != reflect.Pointer {
		return nil
	}
	return root
}

func hasControlFlow(root *stmt.Block) bool {
	if root == nil {
		return false
	}
	for _, statement := range root.Statements() {
		switch statement.(type) {
		case *stmt.If, *stmt.ForEach, *stmt.ForLoop, *stmt.Statement, *stmt.Evaluate, *stmt.Break:
			return true
		}
	}
	return false
}

func usesVariable(statements []ast.Statement, name string) bool {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *expr.Select:
			if expressionUsesVariable(actual, name) {
				return true
			}
		case *stmt.Statement:
			if expressionUsesVariable(actual.X, name) || expressionUsesVariable(actual.Y, name) {
				return true
			}
		case *stmt.If:
			if expressionUsesVariable(actual.Condition, name) || usesVariable(actual.Body.Statements(), name) {
				return true
			}
			for branch := actual.Else; branch != nil; branch = branch.Else {
				if expressionUsesVariable(branch.Condition, name) || usesVariable(branch.Body.Statements(), name) {
					return true
				}
			}
		case *stmt.ForEach:
			if expressionUsesVariable(actual.Set, name) || usesVariable(actual.Statements(), name) {
				return true
			}
		case *stmt.ForLoop:
			if expressionUsesVariable(actual.Cond, name) || usesVariable(actual.Statements(), name) {
				return true
			}
		case *stmt.Evaluate:
			if expressionUsesVariable(actual.X, name) {
				return true
			}
		}
	}
	return false
}

func expressionUsesVariable(expression ast.Expression, name string) bool {
	if expression == nil {
		return false
	}
	switch actual := expression.(type) {
	case *expr.Select:
		return actual.ID == name || expressionUsesVariable(actual.X, name)
	case *expr.Call:
		if expressionUsesVariable(actual.X, name) {
			return true
		}
		for _, argument := range actual.Args {
			if expressionUsesVariable(argument, name) {
				return true
			}
		}
	case *expr.Binary:
		return expressionUsesVariable(actual.X, name) || expressionUsesVariable(actual.Y, name)
	case *expr.SliceIndex:
		return expressionUsesVariable(actual.X, name) || expressionUsesVariable(actual.Y, name)
	case *expr.Parentheses:
		return expressionUsesVariable(actual.P, name)
	case *expr.Unary:
		return expressionUsesVariable(actual.X, name)
	case *expr.Range:
		return expressionUsesVariable(actual.X, name) || expressionUsesVariable(actual.Y, name)
	}
	return false
}

// Dynamic evaluation inherits the parent selectors, and loop initializer/post
// expressions are not covered by usesVariable. Retain existing full declarations
// for these constructs instead of guessing which roots they may need.
func requiresFullInputDeclarations(statements []ast.Statement) bool {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *stmt.Evaluate, *stmt.ForLoop:
			return true
		case *stmt.If:
			if requiresFullInputDeclarations(actual.Body.Statements()) {
				return true
			}
			for branch := actual.Else; branch != nil; branch = branch.Else {
				if requiresFullInputDeclarations(branch.Body.Statements()) {
					return true
				}
			}
		case *stmt.ForEach:
			if requiresFullInputDeclarations(actual.Statements()) {
				return true
			}
		}
	}
	return false
}
