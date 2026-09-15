// Package constant owns immutable, application-supplied constant values.
// It never reads request values or changes authored metadata.
package constant

import (
	"crypto/sha256"
	"fmt"
	"go/token"
	"io/fs"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/viant/datly/spec"
)

// Values is a frozen, case-insensitive set. Constructors copy all caller maps.
// Values are strings to retain the existing typed-constant conversion boundary
// and preserve integer precision when loading JSON or YAML numbers.
type Values struct{ values map[string]string }

func New(values map[string]string) (*Values, error) {
	result := &Values{values: map[string]string{}}
	for name, value := range values {
		if !token.IsIdentifier(name) || name == "_" {
			return nil, fmt.Errorf("constant name %q must be an identifier", name)
		}
		key := strings.ToLower(name)
		switch key {
		case "unsafe", "view", "predicate", "criteria", "sqlbindings", "sqlinstanceconstants":
			return nil, fmt.Errorf("constant name %q is reserved for SQL templates", name)
		}
		if _, ok := result.values[key]; ok {
			return nil, fmt.Errorf("ambiguous constant name %q", name)
		}
		result.values[key] = value
	}
	return result, nil
}

func (v *Values) Lookup(name string) (string, bool) {
	if v == nil {
		return "", false
	}
	value, ok := v.values[strings.ToLower(name)]
	return value, ok
}

// For overlays these instance values on validated authored defaults. Missing
// instance keys retain defaults; an empty value is an explicit override.
func (v *Values) For(component *spec.Component) (*Values, error) {
	defaults := map[string]string{}
	if component != nil {
		if component.Settings != nil {
			for k, value := range component.Settings.Const {
				defaults[k] = value
			}
		}
		for _, p := range spec.EffectiveParameters(component.Parameters) {
			if p == nil {
				continue
			}
			if !strings.EqualFold(p.Source.Kind, "const") {
				if _, found := v.Lookup(p.Name); found {
					return nil, fmt.Errorf("constant %q conflicts with a non-const parameter", p.Name)
				}
				for key := range defaults {
					if strings.EqualFold(key, p.Name) {
						return nil, fmt.Errorf("constant %q conflicts with a non-const parameter", p.Name)
					}
				}
				continue
			}
			if p.Value == nil {
				continue
			}
			name := p.Source.Name
			if name == "" {
				name = p.Name
			}
			for key := range defaults {
				if strings.EqualFold(key, name) {
					if defaults[key] != *p.Value {
						return nil, fmt.Errorf("authored constant %q has conflicting defaults", name)
					}
					delete(defaults, key)
				}
			}
			defaults[name] = *p.Value
		}
	}
	if len(defaults) == 0 && v == nil {
		return nil, nil
	}
	result, err := New(defaults)
	if err != nil {
		return nil, err
	}
	if v != nil {
		for k, value := range v.values {
			result.values[k] = value
		}
	}
	return result, nil
}

// Apply returns runtime-only metadata with externally overridden const values.
// Call this after authored duplicate/conflict checks and never before generation.
func (v *Values) Apply(component *spec.Component) *spec.Component {
	result := component.Clone()
	if result == nil || v == nil {
		return result
	}
	if result.Settings != nil {
		for k := range result.Settings.Const {
			if value, ok := v.Lookup(k); ok {
				result.Settings.Const[k] = value
			}
		}
	}
	for _, p := range spec.EffectiveParameters(result.Parameters) {
		if p == nil || !strings.EqualFold(p.Source.Kind, "const") {
			continue
		}
		name := p.Source.Name
		if name == "" {
			name = p.Name
		}
		if value, ok := v.Lookup(name); ok {
			p.Value = &value
		}
	}
	return result
}

// Path expands only exact constant tokens in a trusted path/URL argument.
// It is single-pass: dollar signs inside values never become further tokens.
func (v *Values) Path(source string) (string, error) {
	if v == nil {
		return source, nil
	}
	var result strings.Builder
	for i := 0; i < len(source); {
		if source[i] != '$' {
			result.WriteByte(source[i])
			i++
			continue
		}
		start := i
		i++
		braced := i < len(source) && source[i] == '{'
		if braced {
			i++
		}
		begin := i
		for i < len(source) {
			ch, size := utf8.DecodeRuneInString(source[i:])
			if ch != '_' && !unicode.IsLetter(ch) && !(i > begin && unicode.IsDigit(ch)) {
				break
			}
			i += size
		}
		if begin == i || braced && (i == len(source) || source[i] != '}') {
			return "", fmt.Errorf("malformed constant reference at byte %d", start)
		}
		name := source[begin:i]
		if braced {
			i++
		}
		value, ok := v.Lookup(name)
		if !ok {
			return "", fmt.Errorf("unknown constant %q", name)
		}
		if strings.ContainsRune(value, 0) {
			return "", fmt.Errorf("constant %q contains NUL", name)
		}
		result.WriteString(value)
	}
	return result.String(), nil
}

// Resources resolves a path at Open while retaining the supplied filesystem's
// namespace and root authority. It neither caches contents nor rewrites files.
func (v *Values) Resources(source fs.FS) fs.FS {
	if v == nil || source == nil {
		return source
	}
	return &files{values: v, source: source}
}

type files struct {
	values *Values
	source fs.FS
}

func (f *files) Open(name string) (fs.File, error) {
	resolved, err := f.values.Path(name)
	if err != nil {
		return nil, err
	}
	return f.source.Open(resolved)
}

func (v *Values) Empty() bool { return v == nil || len(v.values) == 0 }

// Names returns a sorted detached list of canonical names.
func (v *Values) Names() []string {
	if v == nil {
		return nil
	}
	names := make([]string, 0, len(v.values))
	for name := range v.values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Identity partitions native cache namespaces without exposing configured values.
func (v *Values) Identity() string {
	if v.Empty() {
		return ""
	}
	h := sha256.New()
	for _, name := range v.Names() {
		fmt.Fprintf(h, "%d:%s%d:%s", len(name), name, len(v.values[name]), v.values[name])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
