package generate

import (
	"fmt"
	"go/token"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/viant/datly/spec"
)

// resolveClientInput copies only public transport declarations. It does not
// replace the runtime input holder or change any parameter binding policy.
func (r *planResolver) resolveClientInput() error {
	if r.plan.Generation == nil || r.plan.Generation.ClientInputType == "" {
		return nil
	}
	name := r.plan.Generation.ClientInputType
	first, _ := utf8.DecodeRuneInString(name)
	if !token.IsIdentifier(name) || !unicode.IsUpper(first) {
		return fmt.Errorf("client input type %q must be an exported local Go identifier", name)
	}
	allowed := map[string]bool{}
	for _, param := range r.input.Component.Parameters {
		if publicClientParameter(param) {
			allowed[generatedParameterName(param)] = true
		}
	}
	client := &ContractPlan{Package: r.plan.Package, Type: name, Destination: r.plan.Generation.File("client_input", "client_input.go"), Ownership: ContractGenerated}
	// Use resolved canonical fields, not a second type inference implementation.
	for _, field := range r.plan.Input.Fields {
		if !allowed[field.Name] || shouldSkipHasMirror(field) || field.Source == "output" {
			continue
		}
		client.Fields = append(client.Fields, field)
	}
	if err := applyInferredJSONTags(&Plan{Settings: r.plan.Settings}, client.Fields); err != nil {
		return fmt.Errorf("generated client input %q: %w", name, err)
	}
	r.plan.ClientInput = client
	return nil
}

func publicClientParameter(param *spec.Parameter) bool {
	if param == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(param.Source.Kind)) {
	case "path", "query", "body", "header", "cookie", "form":
	default:
		return false
	}
	tag := reflect.StructTag(param.Tag)
	if strings.EqualFold(tag.Get("internal"), "true") || strings.EqualFold(tag.Get("private"), "true") {
		return false
	}
	return !strings.EqualFold(param.Scope, "private") && !strings.EqualFold(param.Scope, "internal")
}
