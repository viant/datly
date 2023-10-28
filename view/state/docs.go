package state

import "strings"

type (
	Docs struct {
		Filter     Documentation
		Columns    Documentation
		Output     Documentation
		Parameters Documentation
	}
	Documentation map[string]interface{}
)

func (d Documentation) ColumnDocumentation(table, column string) (string, bool) {
	if tableDoc, ok := d.FieldDocumentation(table); ok {
		result, ok := tableDoc.ByName(column)
		if ok {
			return result, ok
		}
	}

	if table != "" {
		description, ok := d.ByName(table + "." + column)
		if ok {
			return description, true
		}
	}

	return d.ByName(column)
}

func (d Documentation) FieldDocumentation(name string) (Documentation, bool) {
	result, ok := d[name]
	if !ok {
		return nil, false
	}

	asDoc, ok := result.(Documentation)
	return asDoc, ok
}

func (d Documentation) ByName(name string) (string, bool) {
	result, ok := d[name]
	if ok {
		asString, ok := d.toString(result)
		if ok {
			return asString, ok
		}
	}

	for key, value := range d {
		if strings.EqualFold(key, name) {
			return d.toString(value)
		}
	}

	return "", false
}

func (d Documentation) toString(result interface{}) (string, bool) {
	fieldDoc, ok := result.(string)
	if ok {
		return fieldDoc, true
	}

	documentation, ok := result.(Documentation)
	if ok {
		return documentation.holderDescription()
	}
	return "", false
}

func (d Documentation) holderDescription() (string, bool) {
	result, ok := d["_"]
	if !ok {
		return "", false
	}

	doc, ok := result.(string)
	return doc, ok
}
