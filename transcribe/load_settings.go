package transcribe

import (
	"strings"

	"github.com/viant/datly/spec"
)

type settingsLoader struct {
	base     *spec.Settings
	authored *spec.Settings
}

func (l *settingsLoader) Load() *spec.Settings {
	if l.base == nil {
		return l.authored.Clone()
	}
	result := l.base.Clone()
	if l.authored == nil {
		return result
	}
	if len(l.authored.MCPFolders) > 0 {
		result.MCPFolders = append([]spec.ResourceFolder(nil), l.authored.MCPFolders...)
		for i := range result.MCPFolders {
			result.MCPFolders[i] = result.MCPFolders[i].Clone()
		}
	}
	l.setString(&result.DefaultConnector, l.authored.DefaultConnector)
	result.Generation = l.mergeGeneration(result.Generation, l.authored.Generation)
	l.setString(&result.InputType, l.authored.InputType)
	l.setString(&result.OutputType, l.authored.OutputType)
	l.setString(&result.JSONMarshalType, l.authored.JSONMarshalType)
	l.setString(&result.JSONUnmarshalType, l.authored.JSONUnmarshalType)
	l.setString(&result.XMLUnmarshalType, l.authored.XMLUnmarshalType)
	l.setString(&result.Format, l.authored.Format)
	l.setString(&result.DateFormat, l.authored.DateFormat)
	l.setString(&result.CaseFormat, l.authored.CaseFormat)
	result.Report = l.mergeReport(result.Report, l.authored.Report)
	result.Cache = l.mergeCache(result.Cache, l.authored.Cache)
	if len(l.authored.Const) > 0 {
		if result.Const == nil {
			result.Const = map[string]string{}
		}
		for key, value := range l.authored.Const {
			for inherited := range result.Const {
				if strings.EqualFold(strings.TrimSpace(inherited), strings.TrimSpace(key)) {
					delete(result.Const, inherited)
				}
			}
			result.Const[key] = value
		}
	}
	return result
}

func (l *settingsLoader) mergeGeneration(base, authored *spec.GenerationSettings) *spec.GenerationSettings {
	if authored == nil {
		return base
	}
	if base == nil {
		base = &spec.GenerationSettings{}
	} else {
		base = base.Clone()
	}
	l.setString(&base.Template, authored.Template)
	l.setString(&base.DescriptionResource, authored.DescriptionResource)
	l.setString(&base.ViewFile, authored.ViewFile)
	l.setString(&base.InputFile, authored.InputFile)
	l.setString(&base.OutputFile, authored.OutputFile)
	l.setString(&base.RouterFile, authored.RouterFile)
	l.setString(&base.FilePrefix, authored.FilePrefix)
	l.setString(&base.HandlerFile, authored.HandlerFile)
	l.setString(&base.LifecycleFile, authored.LifecycleFile)
	l.setString(&base.MutationFile, authored.MutationFile)
	l.setString(&base.ResourcesFile, authored.ResourcesFile)
	l.setString(&base.LinksFile, authored.LinksFile)
	l.setString(&base.TemplateFile, authored.TemplateFile)
	if len(authored.SupportFiles) > 0 {
		if base.SupportFiles == nil {
			base.SupportFiles = map[string]string{}
		}
		for role, file := range authored.SupportFiles {
			base.SupportFiles[role] = file
		}
	}
	if base.IsZero() {
		return nil
	}
	return base
}

func (l *settingsLoader) setString(target *string, authored string) {
	if strings.TrimSpace(authored) != "" {
		*target = authored
	}
}

func (l *settingsLoader) mergeReport(base, authored *spec.ReportSettings) *spec.ReportSettings {
	if authored == nil {
		return base
	}
	if base == nil {
		base = &spec.ReportSettings{}
	} else {
		base = base.Clone()
	}
	// A cubeCompose-only DQL overlay configures composition and must not disable
	// a package-owned cube. DQL currently has no report-disable syntax.
	if authored.Enabled {
		base.Enabled = true
	}
	if authored.Compose != nil {
		base.Compose = authored.Compose.Clone()
	}
	if authored.MCPTool != nil {
		enabled := *authored.MCPTool
		base.MCPTool = &enabled
	}
	// The authored report directive always carries its first positional argument.
	// An explicit empty value selects generated report input and must clear a
	// package-provided linked type.
	base.LinkedInputType = strings.TrimSpace(authored.LinkedInputType)
	base.InputLayout = l.mergeReportInputLayout(base.InputLayout, authored.InputLayout)
	return base
}

func (l *settingsLoader) mergeReportInputLayout(base, authored *spec.ReportInputLayout) *spec.ReportInputLayout {
	if authored == nil {
		return base
	}
	if base == nil {
		base = &spec.ReportInputLayout{}
	} else {
		base = base.Clone()
	}
	l.setString(&base.Dimensions, authored.Dimensions)
	l.setString(&base.Measures, authored.Measures)
	l.setString(&base.Filters, authored.Filters)
	l.setString(&base.OrderBy, authored.OrderBy)
	l.setString(&base.Limit, authored.Limit)
	l.setString(&base.Offset, authored.Offset)
	return base
}

func (l *settingsLoader) mergeCache(base, authored *spec.CacheSettings) *spec.CacheSettings {
	if authored == nil {
		return base
	}
	if !authored.Enabled {
		return &spec.CacheSettings{}
	}
	if base == nil {
		base = &spec.CacheSettings{}
	} else {
		copy := *base
		copy.Warmup = base.Warmup.Clone()
		base = &copy
	}
	base.Enabled = true
	l.setString(&base.Name, authored.Name)
	l.setString(&base.TTL, authored.TTL)
	l.setString(&base.Provider, authored.Provider)
	l.setString(&base.Location, authored.Location)
	if authored.TimeToLiveMs != 0 {
		base.TimeToLiveMs = authored.TimeToLiveMs
	}
	if authored.Warmup != nil {
		base.Warmup = authored.Warmup.Clone()
	}
	return base
}
