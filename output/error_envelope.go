package output

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	legacyresponse "github.com/viant/xdatly/handler/response"
)

// errorEnvelope retains only immutable, checked output-member authority.
type errorEnvelope struct {
	root    reflect.Type
	status  *shape.Accessor
	message *shape.Accessor
	errors  *shape.Accessor
}

func compileErrorEnvelope(input CompileInput) (*errorEnvelope, error) {
	if input.Component == nil || input.Type == nil {
		return nil, nil
	}
	root := input.Type
	if root.Kind() == reflect.Pointer {
		root = root.Elem()
	}
	if root.Kind() != reflect.Struct {
		return nil, nil
	}
	owner := shape.Linked(root)
	var declared []*spec.Parameter
	var legacy *spec.Parameter
	for _, parameter := range spec.EffectiveParameters(input.Component.Parameters) {
		if parameter == nil || !(parameter.EmitOutput || strings.EqualFold(strings.TrimSpace(parameter.Source.Kind), "output")) ||
			!strings.EqualFold(strings.TrimSpace(parameter.Source.Name), "status") {
			continue
		}
		declared = append(declared, parameter)
		field, err := owner.StructField(strings.TrimSpace(parameter.Name))
		if err != nil {
			continue
		}
		if field.Type == reflect.TypeFor[legacyresponse.Status]() || field.Type == reflect.TypeFor[*legacyresponse.Status]() {
			legacy = parameter
		}
	}
	if legacy == nil {
		return nil, nil
	}
	if len(declared) != 1 {
		return nil, fmt.Errorf("legacy error output requires one unambiguous declared status slot")
	}
	name := strings.TrimSpace(legacy.Name)
	status, err := owner.Accessor(name + ".Status")
	if err != nil {
		return nil, fmt.Errorf("compile legacy error status %s: %w", name, err)
	}
	message, err := owner.Accessor(name + ".Message")
	if err != nil {
		return nil, fmt.Errorf("compile legacy error message %s: %w", name, err)
	}
	errors, err := owner.Accessor(name + ".Errors")
	if err != nil {
		return nil, fmt.Errorf("compile legacy error details %s: %w", name, err)
	}
	result := &errorEnvelope{root: input.Type, status: status, message: message, errors: errors}
	// Check writable authority once; this value is discarded, never retained.
	if _, err = result.fresh("", []*legacyresponse.Error{{}}); err != nil {
		return nil, fmt.Errorf("compile legacy error output: %w", err)
	}
	return result, nil
}

func (e *errorEnvelope) fresh(message string, details []*legacyresponse.Error) (any, error) {
	root := e.root
	if root.Kind() == reflect.Pointer {
		root = root.Elem()
	}
	value := reflect.New(root)
	if err := e.status.Set(value.Interface(), "error"); err != nil {
		return nil, err
	}
	if err := e.message.Set(value.Interface(), message); err != nil {
		return nil, err
	}
	if err := e.errors.Set(value.Interface(), detailsValue(details)); err != nil {
		return nil, err
	}
	if e.root.Kind() == reflect.Pointer {
		return value.Convert(e.root).Interface(), nil
	}
	return value.Elem().Interface(), nil
}

// ErrorEnvelope creates a fresh declared legacy output from already projected
// public text. False means this output has no eligible legacy status authority.
func (p *Plan) ErrorEnvelope(message string) (any, bool, error) {
	if p == nil || p.errorEnvelope == nil {
		return nil, false, nil
	}
	value, err := p.errorEnvelope.fresh(message, nil)
	return value, true, err
}

// ErrorEnvelopeWithCause adds source-owned binding details to the existing
// declared legacy output. message and statusCode are already selected public
// values; unsupported cause graphs retain the plain ErrorEnvelope behavior.
func (p *Plan) ErrorEnvelopeWithCause(message string, statusCode int, cause error) (any, bool, error) {
	if p == nil || p.errorEnvelope == nil {
		return nil, false, nil
	}
	details, ok := projectBindingErrors(cause, statusCode)
	if !ok {
		return p.ErrorEnvelope(message)
	}
	value, err := p.errorEnvelope.fresh(message, details)
	return value, true, err
}
func detailsValue(details []*legacyresponse.Error) any {
	if details == nil {
		return nil
	}
	return details
}
