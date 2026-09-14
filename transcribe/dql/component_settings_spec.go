package dql

import (
	"strings"

	"github.com/viant/datly/spec"
)

func toSpecSettings(input *componentSettings) *spec.Settings {
	if input == nil {
		return nil
	}
	ret := &spec.Settings{
		MCPFolders:                 append([]spec.ResourceFolder(nil), input.MCPFolders...),
		DefaultConnector:           strings.TrimSpace(input.DefaultConnector),
		Report:                     input.Report,
		Cache:                      input.Cache,
		InputType:                  strings.TrimSpace(input.InputType),
		OutputType:                 strings.TrimSpace(input.OutputType),
		JSONMarshalType:            strings.TrimSpace(input.JSONMarshalType),
		JSONUnmarshalType:          strings.TrimSpace(input.JSONUnmarshalType),
		XMLUnmarshalType:           strings.TrimSpace(input.XMLUnmarshalType),
		Format:                     strings.TrimSpace(input.Format),
		DateFormat:                 strings.TrimSpace(input.DateFormat),
		IgnoreEmptyQueryParameters: input.IgnoreEmptyQueryParameters,
		CaseFormat:                 strings.TrimSpace(input.CaseFormat),
		Output:                     (&spec.Settings{Output: input.Output}).Clone().Output,
		Const:                      cloneStringMap(input.Const),
	}
	generation := input.Generation
	for i := range ret.MCPFolders {
		ret.MCPFolders[i] = ret.MCPFolders[i].Clone()
	}
	generation.Template = strings.TrimSpace(generation.Template)
	generation.DescriptionResource = strings.TrimSpace(generation.DescriptionResource)
	generation.ViewFile = strings.TrimSpace(generation.ViewFile)
	generation.InputFile = strings.TrimSpace(generation.InputFile)
	generation.OutputFile = strings.TrimSpace(generation.OutputFile)
	generation.RouterFile = strings.TrimSpace(generation.RouterFile)
	if !generation.IsZero() {
		ret.Generation = &generation
	}
	if len(ret.MCPFolders) == 0 && ret.Generation == nil && ret.DefaultConnector == "" && ret.Report == nil && ret.Cache == nil &&
		ret.InputType == "" && ret.OutputType == "" &&
		ret.JSONMarshalType == "" && ret.JSONUnmarshalType == "" && ret.XMLUnmarshalType == "" &&
		ret.Format == "" && ret.DateFormat == "" && ret.CaseFormat == "" && ret.Output == nil &&
		len(ret.Const) == 0 && ret.IgnoreEmptyQueryParameters == nil {
		return nil
	}
	return ret
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	ret := make(map[string]string, len(input))
	for k, v := range input {
		ret[k] = v
	}
	return ret
}
