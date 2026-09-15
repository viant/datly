package tag

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

const (
	SequenceStrategyTag = "sequenceStrategy"
	CaseFormatTag       = "caseFormat"
	CacheTag            = "cache"
	JSONMarshalTag      = "jsonMarshal"
	JSONUnmarshalTag    = "jsonUnmarshal"
	XMLUnmarshalTag     = "xmlUnmarshal"
	FormatTag           = "format"
	DateFormatTag       = "dateFormat"
	OutputSettingsTag   = "output"
	IgnoreEmptyQueryTag = "ignoreEmptyQueryParameters"
)

// Settings contains component settings that remain meaningful after package
// generation. spec.Settings.Generation, type names, documentation sources, and
// constants are deliberately excluded because transcription consumes or
// materializes them before package bootstrap.
type Settings struct {
	SequenceStrategy           string
	MCPFolders                 []spec.ResourceFolder
	IgnoreEmptyQueryParameters *bool
	CaseFormat                 string
	Cache                      *spec.CacheSettings
	JSONMarshalType            string
	JSONUnmarshalType          string
	XMLUnmarshalType           string
	Format                     string
	DateFormat                 string
	Output                     *spec.OutputSettings
}

func SettingsFromSpec(source *spec.Settings) Settings {
	if source == nil {
		return Settings{}
	}
	cloned := source.Clone()
	return Settings{
		SequenceStrategy:           cloned.SequenceStrategy,
		MCPFolders:                 cloned.MCPFolders,
		IgnoreEmptyQueryParameters: cloned.IgnoreEmptyQueryParameters,
		CaseFormat:                 cloned.CaseFormat, Cache: cloned.Cache,
		JSONMarshalType: cloned.JSONMarshalType, JSONUnmarshalType: cloned.JSONUnmarshalType,
		XMLUnmarshalType: cloned.XMLUnmarshalType, Format: cloned.Format, DateFormat: cloned.DateFormat,
		Output: cloned.Output,
	}
}

func (s Settings) Apply(target *spec.Settings) {
	if target == nil {
		return
	}
	target.SequenceStrategy = s.SequenceStrategy
	target.CaseFormat = s.CaseFormat
	target.MCPFolders = append([]spec.ResourceFolder(nil), s.MCPFolders...)
	for i := range target.MCPFolders {
		target.MCPFolders[i] = target.MCPFolders[i].Clone()
	}
	if s.IgnoreEmptyQueryParameters != nil {
		value := *s.IgnoreEmptyQueryParameters
		target.IgnoreEmptyQueryParameters = &value
	}
	target.JSONMarshalType = s.JSONMarshalType
	target.JSONUnmarshalType = s.JSONUnmarshalType
	target.XMLUnmarshalType = s.XMLUnmarshalType
	target.Format = s.Format
	target.DateFormat = s.DateFormat
	if s.Output != nil {
		target.Output = (&spec.Settings{Output: s.Output}).Clone().Output
	}
	if s.Cache != nil {
		target.Cache = (&spec.Settings{Cache: s.Cache}).Clone().Cache
	}
}

func (s Settings) StructTag() (string, error) {
	if err := (&spec.Settings{SequenceStrategy: s.SequenceStrategy}).ValidateSequenceStrategy(); err != nil {
		return "", err
	}
	var tags []string
	appendValue := func(name, value string) {
		if value != "" {
			tags = append(tags, name+":"+strconv.Quote(value))
		}
	}
	appendValue(SequenceStrategyTag, s.SequenceStrategy)
	appendValue(CaseFormatTag, s.CaseFormat)
	if len(s.MCPFolders) > 0 {
		data, err := json.Marshal(s.MCPFolders)
		if err != nil {
			return "", err
		}
		appendValue("mcpFolders", string(data))
	}
	if s.IgnoreEmptyQueryParameters != nil {
		appendValue(IgnoreEmptyQueryTag, strconv.FormatBool(*s.IgnoreEmptyQueryParameters))
	}
	if s.Cache != nil {
		value, err := json.Marshal(s.Cache)
		if err != nil {
			return "", fmt.Errorf("marshal cache tag: %w", err)
		}
		appendValue(CacheTag, string(value))
	}
	appendValue(JSONMarshalTag, s.JSONMarshalType)
	appendValue(JSONUnmarshalTag, s.JSONUnmarshalType)
	appendValue(XMLUnmarshalTag, s.XMLUnmarshalType)
	appendValue(FormatTag, s.Format)
	appendValue(DateFormatTag, s.DateFormat)
	if s.Output != nil {
		value, err := json.Marshal(s.Output)
		if err != nil {
			return "", err
		}
		appendValue(OutputSettingsTag, string(value))
	}
	return strings.Join(tags, " "), nil
}

func ParseSettings(structTag reflect.StructTag) (Settings, error) {
	result := Settings{
		SequenceStrategy: structTag.Get(SequenceStrategyTag),
		CaseFormat:       structTag.Get(CaseFormatTag), JSONMarshalType: structTag.Get(JSONMarshalTag),
		JSONUnmarshalType: structTag.Get(JSONUnmarshalTag), XMLUnmarshalType: structTag.Get(XMLUnmarshalTag),
		Format: structTag.Get(FormatTag), DateFormat: structTag.Get(DateFormatTag),
	}
	if value, ok := structTag.Lookup("mcpFolders"); ok {
		if err := json.Unmarshal([]byte(value), &result.MCPFolders); err != nil {
			return Settings{}, err
		}
		for _, folder := range result.MCPFolders {
			if err := folder.Validate(); err != nil {
				return Settings{}, err
			}
		}
	}
	if value, ok := structTag.Lookup(IgnoreEmptyQueryTag); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Settings{}, fmt.Errorf("parse empty-query policy: %w", err)
		}
		result.IgnoreEmptyQueryParameters = &parsed
	}
	if value, ok := structTag.Lookup(CacheTag); ok {
		result.Cache = &spec.CacheSettings{}
		if err := json.Unmarshal([]byte(value), result.Cache); err != nil {
			return Settings{}, fmt.Errorf("parse cache tag: %w", err)
		}
	}
	if value, ok := structTag.Lookup(OutputSettingsTag); ok {
		result.Output = &spec.OutputSettings{}
		if err := json.Unmarshal([]byte(value), result.Output); err != nil {
			return Settings{}, fmt.Errorf("parse output tag: %w", err)
		}
	}
	if err := (&spec.Settings{SequenceStrategy: result.SequenceStrategy}).ValidateSequenceStrategy(); err != nil {
		return Settings{}, err
	}
	return result, nil
}
