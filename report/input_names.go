package report

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// lowerCamel names cube inputs independently of Go fields and reader JSON tags.
func lowerCamel(value string) string {
	letters := []rune(value)
	var result strings.Builder
	result.Grow(len(value))
	appendWord := func(start, end int) {
		if start == end {
			return
		}
		if result.Len() == 0 {
			result.WriteString(strings.ToLower(string(letters[start:end])))
			return
		}
		result.WriteRune(unicode.ToUpper(letters[start]))
		result.WriteString(strings.ToLower(string(letters[start+1 : end])))
	}
	start := 0
	for i, letter := range letters {
		if !unicode.IsLetter(letter) && !unicode.IsDigit(letter) {
			appendWord(start, i)
			start = i + 1
			continue
		}
		if i == start || !unicode.IsUpper(letter) {
			continue
		}
		previous := letters[i-1]
		boundary := unicode.IsLower(previous) || unicode.IsDigit(previous)
		if unicode.IsUpper(previous) && i+1 < len(letters) && unicode.IsLower(letters[i+1]) {
			// Keep the plural suffix with its acronym (IDs, URLs), but split
			// the final capital before a word: VExposed -> V + Exposed.
			plural := letters[i+1] == 's' && (i+2 == len(letters) || !unicode.IsLower(letters[i+2]))
			boundary = !plural
		}
		if boundary {
			appendWord(start, i)
			start = i
		}
	}
	appendWord(start, len(letters))
	return result.String()
}

type inputWireNames map[string]map[string]string

func (n inputWireNames) add(section, field, wire string) error {
	if n[section] == nil {
		n[section] = map[string]string{}
	}
	if previous, ok := n[section][wire]; ok && previous != field {
		return fmt.Errorf("cube input section %s fields %q and %q share wire name %q", section, previous, field, wire)
	}
	n[section][wire] = field
	return nil
}

func (n inputWireNames) addField(path string, field reflect.StructField) error {
	wire := strings.Split(field.Tag.Get("json"), ",")[0]
	if wire == "-" {
		return nil
	}
	if wire == "" {
		wire = field.Name
	}
	section := path[:strings.LastIndex(path, ".")+1]
	return n.add(strings.TrimSuffix(section, "."), path, wire)
}

func (m *metadata) validateWireNames() error {
	names := inputWireNames{}
	for _, section := range []struct {
		name   string
		fields []field
	}{{m.inputLayout.Dimensions, m.dimensions}, {m.inputLayout.Measures, m.measures}} {
		for _, item := range section.fields {
			if err := names.add(section.name, item.fieldName, selectionWireName(item)); err != nil {
				return err
			}
		}
	}
	for _, item := range m.filters {
		if err := names.add(m.inputLayout.Filters, item.fieldName, selectionWireName(item.field)); err != nil {
			return err
		}
	}
	return nil
}
