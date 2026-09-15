package tag

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/viant/datly/spec"
)

// Method definitions are serialized typed selector metadata. Argument type
// interpretation belongs to the reader compiler's canonical type owner.
func parseSQLMethods(value string) ([]spec.SQLMethod, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var methods []spec.SQLMethod
	if err := decoder.Decode(&methods); err != nil {
		return nil, fmt.Errorf("selector SQL methods: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("selector SQL methods contain trailing data")
	}
	if err := validateSQLMethods(methods); err != nil {
		return nil, err
	}
	return methods, nil
}

// ParseSQLMethods decodes the canonical selector-method JSON used by both Go
// view tags and DQL selector policy declarations.
func ParseSQLMethods(value string) ([]spec.SQLMethod, error) {
	return parseSQLMethods(value)
}

func encodeSQLMethods(methods []spec.SQLMethod) (string, error) {
	if err := validateSQLMethods(methods); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(methods)
	return string(encoded), err
}

func validateSQLMethods(methods []spec.SQLMethod) error {
	seen := map[string]bool{}
	for _, method := range methods {
		name := strings.ToLower(strings.TrimSpace(method.Name))
		if name == "" || seen[name] {
			return fmt.Errorf("selector SQL method %q is missing or duplicated", method.Name)
		}
		seen[name] = true
		for _, argument := range method.Args {
			if strings.TrimSpace(argument) == "" {
				return fmt.Errorf("selector SQL method %s has an empty argument type", method.Name)
			}
		}
	}
	return nil
}
