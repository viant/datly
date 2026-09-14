package compiler

import (
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/format/text"
)

// OutputSlot describes one reserved output slot: the logical output name used
// by `output/...` params and the conventional exported field name used when no
// param claims the slot explicitly.
type OutputSlot struct {
	OutputName string
	FieldName  string
}

var (
	ViewSlot    = OutputSlot{OutputName: "view", FieldName: "Data"}
	StatusSlot  = OutputSlot{OutputName: "status", FieldName: "Status"}
	MetricsSlot = OutputSlot{OutputName: "metrics", FieldName: "Metrics"}
)

// ResolveOutputField resolves the exported output-struct field backing a
// reserved output slot. It first honors an explicit output-kind param whose
// Source.Name matches the slot output name (view also accepts body), then uses the slot's conventional
// field when no param claims it.
func ResolveOutputField(component *spec.Component, outputType reflect.Type, slot OutputSlot) string {
	for outputType != nil && outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}
	if outputType == nil || outputType.Kind() != reflect.Struct {
		return ""
	}
	if component != nil {
		for _, param := range component.Parameters {
			if param == nil {
				continue
			}
			name := strings.TrimSpace(param.Source.Name)
			matches := strings.EqualFold(name, slot.OutputName) || slot.OutputName == ViewSlot.OutputName && strings.EqualFold(name, "body")
			if !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") || !matches {
				continue
			}
			if field, ok := typecatalog.FieldByName(outputType, param.Name); ok {
				return field.Name
			}
		}
	}
	if slot.FieldName != "" {
		if component != nil {
			for _, param := range component.Parameters {
				if param == nil {
					continue
				}
				if text.DetectCaseFormat(param.Name).Format(param.Name, text.CaseFormatUpperCamel) != slot.FieldName {
					continue
				}
				if param.EmitOutput || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
					return ""
				}
			}
		}
		if _, ok := outputType.FieldByName(slot.FieldName); ok {
			return slot.FieldName
		}
	}
	return ""
}
