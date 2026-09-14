package transcribe

import (
	"errors"
	"fmt"
	"go/token"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
)

type constantParamError struct {
	name     string
	identity string
	cause    error
}

func (e *constantParamError) Error() string { return e.cause.Error() }
func (e *constantParamError) Unwrap() error { return e.cause }

type constantTableError struct {
	name  string
	cause error
}

func (e *constantTableError) Error() string { return e.cause.Error() }
func (e *constantTableError) Unwrap() error { return e.cause }

func synthesizeConstantParams(component *spec.Component) error {
	if component == nil || component.Settings == nil || len(component.Settings.Const) == 0 {
		return nil
	}
	type constantEntry struct {
		name  string
		value string
	}
	entries := make([]constantEntry, 0, len(component.Settings.Const))
	byName := make(map[string]string, len(component.Settings.Const))
	for rawName, value := range component.Settings.Const {
		name := strings.TrimSpace(rawName)
		if !token.IsIdentifier(name) {
			return fmt.Errorf("constant name %q must be a Go identifier", rawName)
		}
		canonical := strings.ToLower(name)
		if previous := byName[canonical]; previous != "" {
			return fmt.Errorf("constant names %q and %q are ambiguous", previous, name)
		}
		byName[canonical] = name
		entries = append(entries, constantEntry{name: name, value: value})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	existing := map[string]*spec.Parameter{}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "const") {
			continue
		}
		name := strings.TrimSpace(param.Source.Name)
		if name == "" {
			name = strings.TrimSpace(param.Name)
		}
		canonical := strings.ToLower(name)
		if previous := existing[canonical]; previous != nil && previous != param {
			return &constantParamError{name: name, identity: param.Identity(), cause: fmt.Errorf("constant parameter %q is declared more than once", name)}
		}
		existing[canonical] = param
	}
	for _, entry := range entries {
		name, value := entry.name, entry.value
		if param := existing[strings.ToLower(name)]; param != nil {
			if param.Value != nil && *param.Value != value {
				return &constantParamError{name: name, identity: param.Identity(), cause: fmt.Errorf("constant %q has conflicting values %q and %q", name, *param.Value, value)}
			}
			param.Source.Kind = "const"
			param.Source.Name = name
			if param.Value == nil {
				constant := value
				param.Value = &constant
			}
			if strings.TrimSpace(param.TypeExpr) == "" {
				param.TypeExpr = "string"
			}
			continue
		}
		constant := value
		component.Parameters = append(component.Parameters, &spec.Parameter{
			Name: name, Source: spec.BindSource{Kind: "const", Name: name},
			TypeExpr: "string", Value: &constant, Tag: `internal:"true"`,
		})
	}
	return nil
}

func resolveConstantViewTables(view *spec.View, constants map[string]string) error {
	visited := map[*spec.View]bool{}
	var resolve func(*spec.View) error
	resolve = func(current *spec.View) error {
		if current == nil || visited[current] {
			return nil
		}
		visited[current] = true
		if current.Source != nil {
			if name := constantTableName(current.Source.Table); name != "" {
				if value, ok := constantValue(constants, name); ok {
					if value == "" {
						return &constantTableError{name: name, cause: fmt.Errorf("constant table %q resolves to an empty identifier", name)}
					}
					current.Source.Table = value
				}
			}
		}
		for _, relation := range current.Relations {
			if relation != nil {
				if err := resolve(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return resolve(view)
}

func constantTableName(table string) string {
	value := strings.TrimSpace(table)
	if len(value) >= 2 && (value[0] == '`' && value[len(value)-1] == '`' || value[0] == '"' && value[len(value)-1] == '"') {
		return ""
	}
	switch {
	case strings.HasPrefix(value, "${Unsafe.") && strings.HasSuffix(value, "}"):
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "${Unsafe."), "}"))
	case strings.HasPrefix(value, "$Unsafe."):
		return strings.TrimSpace(strings.TrimPrefix(value, "$Unsafe."))
	case strings.HasPrefix(value, "Unsafe_"):
		return strings.TrimSpace(strings.TrimPrefix(value, "Unsafe_"))
	default:
		return ""
	}
}

func constantDiagnosticSpan(prepared *dql.PreparedSource, err error) (dql.SourceSpan, bool) {
	if prepared == nil || prepared.Directives == nil {
		return dql.SourceSpan{}, false
	}
	var paramErr *constantParamError
	if errors.As(err, &paramErr) {
		if span, ok := prepared.Directives.ParamSpans[paramErr.identity]; ok {
			return span, true
		}
		if span, ok := prepared.Directives.ConstSpans[strings.ToLower(strings.TrimSpace(paramErr.name))]; ok {
			return span, true
		}
	}
	var tableErr *constantTableError
	if errors.As(err, &tableErr) {
		span, ok := prepared.Directives.ConstSpans[strings.ToLower(strings.TrimSpace(tableErr.name))]
		return span, ok
	}
	return dql.SourceSpan{}, false
}

func constantValue(constants map[string]string, name string) (string, bool) {
	for key, value := range constants {
		if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(name)) {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}
