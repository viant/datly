package tool

import (
	"fmt"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
	"reflect"
	"strings"
)

// Scalar admission is MCP transport metadata, independent of field naming and
// the canonical HTTP binding. Parse it once while compiling the tool plan.
func scalarAdmission(field reflect.StructField) (bool, error) {
	return booleanAdmission(field, "scalar")
}

func textAdmission(field reflect.StructField) (bool, error) {
	return booleanAdmission(field, "text")
}

func booleanAdmission(field reflect.StructField, option string) (bool, error) {
	_, options := tags.Values(field.Tag.Get("mcp")).Name()
	seen, enabled := false, false
	err := options.MatchPairs(func(key, value string) error {
		if strings.TrimSpace(key) != option {
			return nil
		}
		if seen {
			return fmt.Errorf("duplicate mcp %s option on field %s", option, field.Name)
		}
		seen = true
		switch strings.TrimSpace(value) {
		case "true":
			enabled = true
		case "false":
		default:
			return fmt.Errorf("mcp %s option on field %s must be true or false", option, field.Name)
		}
		return nil
	})
	return enabled, err
}

type mcpField struct {
	name    string
	aliases []string
	hidden  bool
}

func publicFieldName(logicalName, sourceName string, field reflect.StructField) (string, []string, bool, error) {
	if field.Tag.Get("setMarker") == "true" || field.Tag.Get("internal") == "true" {
		return "", nil, true, nil
	}
	mcp, err := mcpFieldMetadata(field)
	if err != nil {
		return "", nil, false, err
	}
	if mcp.hidden {
		return "", nil, true, nil
	}
	if mcp.name != "" {
		return mcp.name, mcp.aliases, false, nil
	}
	if value, ok := field.Tag.Lookup("json"); ok {
		name, _, _ := strings.Cut(value, ",")
		if name == "-" {
			return "", nil, true, nil
		}
		if name != "" {
			return name, mcp.aliases, false, nil
		}
	}
	if logicalName = strings.TrimSpace(logicalName); logicalName != "" {
		return logicalName, mcp.aliases, false, nil
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		return sourceName, mcp.aliases, false, nil
	}
	return field.Name, mcp.aliases, false, nil
}

func jsonField(field reflect.StructField) (name string, hidden bool) {
	if field.Tag.Get("setMarker") == "true" || field.Tag.Get("internal") == "true" {
		return "", true
	}
	name = field.Name
	if value, ok := field.Tag.Lookup("json"); ok {
		parts := strings.Split(value, ",")
		if parts[0] == "-" {
			return "", true
		}
		if parts[0] != "" {
			name = parts[0]
		}
	}
	return name, false
}

func mcpFieldMetadata(field reflect.StructField) (mcpField, error) {
	name, err := fieldname.Resolve(field, "")
	if err != nil {
		return mcpField{}, err
	}
	return mcpField{name: name.Public, aliases: name.Aliases, hidden: name.Hidden}, nil
}

// Explicit JSON and MCP names remain authoritative; caseFormat supplies only
// the otherwise implicit component wire spelling.
func caseFormattedFallback(field reflect.StructField, fallback, format string) string {
	if name, _, _ := strings.Cut(field.Tag.Get("json"), ","); name != "" || strings.TrimSpace(format) == "" {
		return fallback
	}
	return text.DetectCaseFormat(fallback).Format(fallback, text.NewCaseFormat(format))
}
