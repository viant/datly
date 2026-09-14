package transcribe

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

func resolveAuthoredHandler(prepared *dql.PreparedSource, component *spec.Component, goHandler *gen.GoHandlerAsset, supplied *gen.VeltyHandlerAsset) (*gen.VeltyHandlerAsset, error) {
	if goHandler != nil && supplied != nil {
		return nil, fmt.Errorf("supplied Go and Velty handler assets are mutually exclusive")
	}
	if goHandler != nil || supplied != nil {
		return supplied, nil
	}
	return transcribeVeltyHandler(prepared, component)
}

// transcribeVeltyHandler promotes explicitly authored DML service programs to
// the existing Velty handler artifact. Raw SQL DML and mixed read/write source
// require their own parser-backed transcription and are deliberately excluded.
func transcribeVeltyHandler(prepared *dql.PreparedSource, component *spec.Component) (*gen.VeltyHandlerAsset, error) {
	if prepared == nil {
		return nil, nil
	}
	classification := prepared.Statements.Classify()
	if !classification.HasService || classification.HasRead || classification.HasExec {
		return nil, nil
	}
	template := strings.TrimSpace(prepared.SQL)
	if template == "" {
		return nil, nil
	}
	hasService, err := validateVeltyHandlerTemplate(template, component)
	if err != nil {
		return nil, err
	}
	if !hasService {
		return nil, nil
	}
	return &gen.VeltyHandlerAsset{Template: template}, nil
}

func validateVeltyHandlerTemplate(template string, component *spec.Component) (bool, error) {
	root, spans, err := veltyparser.ParseWithSpansDetailed([]byte(template))
	if err != nil {
		return false, fmt.Errorf("invalid Velty handler program: %w", err)
	}
	validator := newVeltyProgramValidator(component, spans)
	if err = validator.validateBlock(root); err != nil {
		return false, err
	}
	return validator.hasService, nil
}

type veltyProgramValidator struct {
	allowed    map[string]bool
	locals     map[string]bool
	spans      map[ast.Node]veltyparser.NodeSpan
	hasService bool
}

func newVeltyProgramValidator(component *spec.Component, spans map[ast.Node]veltyparser.NodeSpan) *veltyProgramValidator {
	allowed := map[string]bool{
		"Input": true, "input": true, "Output": true,
		"dml": true, "index": true, "sequencer": true, "validator": true,
		"writeHooks": true,
		"messageBus": true, "logger": true,
	}
	if component != nil {
		for _, param := range spec.EffectiveParameters(component.Parameters) {
			if param == nil || param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
				continue
			}
			if name := strings.TrimSpace(param.Name); name != "" {
				allowed[name] = true
			}
		}
	}
	return &veltyProgramValidator{allowed: allowed, locals: map[string]bool{}, spans: spans}
}

func (v *veltyProgramValidator) validateBlock(block *stmt.Block) error {
	if block == nil {
		return nil
	}
	for _, item := range block.Statements() {
		if err := v.validateStatement(item); err != nil {
			return err
		}
	}
	return nil
}

func (v *veltyProgramValidator) validateStatement(item ast.Statement) error {
	switch actual := item.(type) {
	case *stmt.Append:
		if len(statement.Parse(actual.Append)) != 0 {
			return v.fail(actual, "Velty handler contains unsupported raw statement %q", strings.TrimSpace(actual.Append))
		}
		return nil
	case *expr.Select:
		return v.validateSelector(actual)
	case *stmt.Statement:
		if err := v.validateExpression(actual.Y); err != nil {
			return err
		}
		left, ok := actual.X.(*expr.Select)
		if !ok {
			return v.validateExpression(actual.X)
		}
		if left.X == nil && !v.selectorAllowed(left.ID) {
			v.locals[left.ID] = true
			return nil
		}
		return v.validateSelector(left)
	case *stmt.Evaluate:
		return v.validateExpression(actual.X)
	case *stmt.If:
		return v.validateIf(actual)
	case *stmt.ForEach:
		if err := v.validateExpression(actual.Set); err != nil {
			return err
		}
		return v.withLocals(selectorID(actual.Index), selectorID(actual.Item), func() error {
			return v.withLocals("foreach", "", func() error {
				return v.validateBlock(&actual.Body)
			})
		})
	case *stmt.ForLoop:
		if err := v.validateStatement(actual.Init); err != nil {
			return err
		}
		if err := v.validateExpression(actual.Cond); err != nil {
			return err
		}
		if err := v.validateBlock(&actual.Body); err != nil {
			return err
		}
		return v.validateStatement(actual.Post)
	case *stmt.Break:
		return nil
	case nil:
		return nil
	default:
		return v.fail(item, "Velty handler contains unsupported statement %T", item)
	}
}

func (v *veltyProgramValidator) validateIf(item *stmt.If) error {
	for current := item; current != nil; current = current.Else {
		if err := v.validateExpression(current.Condition); err != nil {
			return err
		}
		if err := v.validateBlock(&current.Body); err != nil {
			return err
		}
	}
	return nil
}

func (v *veltyProgramValidator) validateExpression(item ast.Expression) error {
	switch actual := item.(type) {
	case *expr.Select:
		return v.validateSelector(actual)
	case *expr.Binary:
		if err := v.validateExpression(actual.X); err != nil {
			return err
		}
		return v.validateExpression(actual.Y)
	case *expr.Unary:
		return v.validateExpression(actual.X)
	case *expr.Parentheses:
		return v.validateExpression(actual.P)
	case *expr.Call:
		if err := v.validateExpression(actual.X); err != nil {
			return err
		}
		for _, arg := range actual.Args {
			if err := v.validateExpression(arg); err != nil {
				return err
			}
		}
		return nil
	case *expr.SliceIndex:
		if err := v.validateExpression(actual.X); err != nil {
			return err
		}
		return v.validateExpression(actual.Y)
	case *expr.Range:
		if err := v.validateExpression(actual.X); err != nil {
			return err
		}
		return v.validateExpression(actual.Y)
	case *expr.Literal, nil:
		return nil
	default:
		return v.fail(item, "Velty handler contains unsupported expression %T", item)
	}
}

func (v *veltyProgramValidator) validateSelector(item *expr.Select) error {
	if item == nil {
		return nil
	}
	if !v.selectorAllowed(item.ID) {
		return v.fail(item, "Velty handler selector %q is not an input, local, or registered capability", item.ID)
	}
	if strings.EqualFold(item.ID, "dml") {
		method, ok := item.X.(*expr.Select)
		if !ok || !statement.IsDMLServiceMethod(method.ID) {
			return v.fail(item, "Velty handler DML method is unsupported")
		}
		call, ok := method.X.(*expr.Call)
		if !ok || call.X != nil {
			return v.fail(method, "Velty handler DML method %q must be called directly", method.ID)
		}
		if err := v.validateDMLCall(method, call); err != nil {
			return err
		}
		for _, arg := range call.Args {
			if err := v.validateExpression(arg); err != nil {
				return err
			}
		}
		v.hasService = true
		return nil
	}
	return v.validateSelectorTail(item.X)
}

func (v *veltyProgramValidator) validateDMLCall(method *expr.Select, call *expr.Call) error {
	if method == nil || call == nil {
		return v.fail(method, "Velty handler DML call is required")
	}
	argumentCount := len(call.Args)
	switch strings.ToLower(strings.TrimSpace(method.ID)) {
	case "insert", "update", "delete":
		if argumentCount != 2 {
			return v.fail(method, "Velty handler DML method %q requires exactly two arguments: table, value", method.ID)
		}
	case "execute":
		if argumentCount == 0 {
			return v.fail(method, "Velty handler DML method %q requires a statement argument", method.ID)
		}
	default:
		return v.fail(method, "Velty handler DML method %q is unsupported", method.ID)
	}
	first, ok := call.Args[0].(*expr.Literal)
	if !ok || first.RType == nil || first.RType.Kind() != reflect.String {
		return v.fail(method, "Velty handler DML method %q requires a string literal as its first argument", method.ID)
	}
	return nil
}

func (v *veltyProgramValidator) validateSelectorTail(item ast.Expression) error {
	switch actual := item.(type) {
	case *expr.Select:
		return v.validateSelectorTail(actual.X)
	case *expr.Call:
		for _, arg := range actual.Args {
			if err := v.validateExpression(arg); err != nil {
				return err
			}
		}
		return v.validateSelectorTail(actual.X)
	case *expr.SliceIndex:
		if err := v.validateExpression(actual.X); err != nil {
			return err
		}
		return v.validateSelectorTail(actual.Y)
	case nil:
		return nil
	default:
		return v.validateExpression(item)
	}
}

func (v *veltyProgramValidator) selectorAllowed(name string) bool {
	return v.allowed[name] || v.locals[name]
}

func (v *veltyProgramValidator) withLocals(first, second string, callback func() error) error {
	added := make([]string, 0, 2)
	for _, name := range []string{first, second} {
		if name == "" || v.locals[name] {
			continue
		}
		v.locals[name] = true
		added = append(added, name)
	}
	err := callback()
	for _, name := range added {
		delete(v.locals, name)
	}
	return err
}

func selectorID(item *expr.Select) string {
	if item == nil {
		return ""
	}
	return item.ID
}

type veltyProgramError struct {
	cause error
	start int
	end   int
}

func (e *veltyProgramError) Error() string { return e.cause.Error() }
func (e *veltyProgramError) Unwrap() error { return e.cause }

func (v *veltyProgramValidator) fail(node ast.Node, format string, args ...any) error {
	result := &veltyProgramError{cause: fmt.Errorf(format, args...)}
	if span, ok := v.spans[node]; ok {
		result.start = span.Start
		result.end = span.End + 1
	}
	return result
}
