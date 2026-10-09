package tag

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

const (
	ResponseCompressionTag  = "responseCompression"
	SequenceStrategyTag     = "sequenceStrategy"
	CaseFormatTag           = "caseFormat"
	CacheTag                = "cache"
	JSONMarshalTag          = "jsonMarshal"
	JSONUnmarshalTag        = "jsonUnmarshal"
	XMLUnmarshalTag         = "xmlUnmarshal"
	FormatTag               = "format"
	DateFormatTag           = "dateFormat"
	OutputSettingsTag       = "output"
	IgnoreEmptyQueryTag     = "ignoreEmptyQueryParameters"
	MutationTag             = "mutation"
	ProtectedFlushTablesTag = "protectedFlushTables"
)

// Settings contains component settings that remain meaningful after package
// generation. spec.Settings.Generation, type names, documentation sources, and
// constants are deliberately excluded because transcription consumes or
// materializes them before package bootstrap.
type Settings struct {
	ProtectedFlushTables         []string
	ComponentCallPolicy          string
	ResponseCompression          *spec.ResponseCompression
	IndependentChildTransactions bool
	Mutation                     string
	SequenceStrategy             string
	MCPFolders                   []spec.ResourceFolder
	IgnoreEmptyQueryParameters   *bool
	CaseFormat                   string
	Cache                        *spec.CacheSettings
	JSONMarshalType              string
	JSONUnmarshalType            string
	XMLUnmarshalType             string
	Format                       string
	DateFormat                   string
	Output                       *spec.OutputSettings
}

func SettingsFromSpec(source *spec.Settings) Settings {
	if source == nil {
		return Settings{}
	}
	cloned := source.Clone()
	return Settings{
		ProtectedFlushTables:         cloned.ProtectedFlushTables,
		ResponseCompression:          cloned.ResponseCompression,
		IndependentChildTransactions: cloned.IndependentChildTransactions,
		ComponentCallPolicy:          cloned.ComponentCallPolicy,
		Mutation:                     cloned.Mutation,
		SequenceStrategy:             cloned.SequenceStrategy,
		MCPFolders:                   cloned.MCPFolders,
		IgnoreEmptyQueryParameters:   cloned.IgnoreEmptyQueryParameters,
		CaseFormat:                   cloned.CaseFormat, Cache: cloned.Cache,
		JSONMarshalType: cloned.JSONMarshalType, JSONUnmarshalType: cloned.JSONUnmarshalType,
		XMLUnmarshalType: cloned.XMLUnmarshalType, Format: cloned.Format, DateFormat: cloned.DateFormat,
		Output: cloned.Output,
	}
}

func (s Settings) Apply(target *spec.Settings) {
	if target == nil {
		return
	}
	target.ProtectedFlushTables = slices.Clone(s.ProtectedFlushTables)
	target.IndependentChildTransactions = s.IndependentChildTransactions
	target.ComponentCallPolicy = s.ComponentCallPolicy
	target.ResponseCompression = s.ResponseCompression.Clone()
	target.SequenceStrategy = s.SequenceStrategy
	target.Mutation = s.Mutation
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
	if err := (&spec.Settings{ProtectedFlushTables: s.ProtectedFlushTables}).ValidateProtectedFlushTables(); err != nil {
		return "", err
	}
	if err := (&spec.Settings{ComponentCallPolicy: s.ComponentCallPolicy, IndependentChildTransactions: s.IndependentChildTransactions}).ValidateComponentCallPolicy(); err != nil {
		return "", err
	}
	if err := validateMutation(s.Mutation); err != nil {
		return "", err
	}
	if err := (&spec.Settings{SequenceStrategy: s.SequenceStrategy}).ValidateSequenceStrategy(); err != nil {
		return "", err
	}
	if err := s.ResponseCompression.Validate(); err != nil {
		return "", err
	}
	var tags []string
	appendValue := func(name, value string) {
		if value != "" {
			tags = append(tags, name+":"+strconv.Quote(value))
		}
	}
	appendValue("componentCallPolicy", s.ComponentCallPolicy)
	if s.ProtectedFlushTables != nil {
		data, err := json.Marshal(s.ProtectedFlushTables)
		if err != nil {
			return "", err
		}
		appendValue(ProtectedFlushTablesTag, string(data))
	}
	if s.IndependentChildTransactions {
		appendValue("independentChildTransactions", "true")
	}
	if s.ResponseCompression != nil {
		data, err := json.Marshal(s.ResponseCompression)
		if err != nil {
			return "", err
		}
		appendValue(ResponseCompressionTag, string(data))
	}
	appendValue(SequenceStrategyTag, s.SequenceStrategy)
	appendValue(MutationTag, s.Mutation)
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
		Mutation:         structTag.Get(MutationTag),
		SequenceStrategy: structTag.Get(SequenceStrategyTag),
		CaseFormat:       structTag.Get(CaseFormatTag), JSONMarshalType: structTag.Get(JSONMarshalTag),
		JSONUnmarshalType: structTag.Get(JSONUnmarshalTag), XMLUnmarshalType: structTag.Get(XMLUnmarshalTag),
		Format: structTag.Get(FormatTag), DateFormat: structTag.Get(DateFormatTag),
	}
	if value, ok := structTag.Lookup(ProtectedFlushTablesTag); ok {
		if err := json.Unmarshal([]byte(value), &result.ProtectedFlushTables); err != nil {
			return Settings{}, fmt.Errorf("parse protected flush tables tag: %w", err)
		}
		if result.ProtectedFlushTables == nil {
			return Settings{}, fmt.Errorf("protected_flush_tables tag requires a nonempty table array")
		}
		if err := (&spec.Settings{ProtectedFlushTables: result.ProtectedFlushTables}).ValidateProtectedFlushTables(); err != nil {
			return Settings{}, err
		}
	}
	if value, ok := structTag.Lookup("componentCallPolicy"); ok {
		if value == "" {
			return Settings{}, fmt.Errorf("component call policy tag cannot be empty")
		}
		result.ComponentCallPolicy = value
	}
	if value, ok := structTag.Lookup("independentChildTransactions"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Settings{}, fmt.Errorf("parse independent child transactions policy: %w", err)
		}
		result.IndependentChildTransactions = parsed
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
	if value, ok := structTag.Lookup(ResponseCompressionTag); ok {
		result.ResponseCompression = &spec.ResponseCompression{}
		if err := json.Unmarshal([]byte(value), result.ResponseCompression); err != nil {
			return Settings{}, fmt.Errorf("parse response compression tag: %w", err)
		}
		if err := result.ResponseCompression.Validate(); err != nil {
			return Settings{}, err
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
	if err := validateMutation(result.Mutation); err != nil {
		return Settings{}, err
	}
	if err := (&spec.Settings{ComponentCallPolicy: result.ComponentCallPolicy, IndependentChildTransactions: result.IndependentChildTransactions}).ValidateComponentCallPolicy(); err != nil {
		return Settings{}, err
	}
	return result, nil
}

func validateMutation(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "patch", "post", "put":
		return nil
	default:
		return fmt.Errorf("unsupported mutation operation %q", value)
	}
}
