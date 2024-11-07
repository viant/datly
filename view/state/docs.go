package state

import (
	"strings"
)

type (
	Docs struct {
		Filter     Documentation
		Columns    Documentation
		Output     Documentation
		Parameters Documentation
	}
	Documentation map[string]interface{}
)

func (d *Docs) Merge(from *Docs) {
	if from == nil {
		return
	}

	if d.Filter == nil {
		d.Filter = make(Documentation)
	}
	d.Filter.Merge(from.Filter)

	if d.Columns == nil {
		d.Columns = make(Documentation)
	}
	d.Columns.Merge(from.Columns)

	if d.Output == nil {
		d.Output = make(Documentation)
	}
	d.Output.Merge(from.Output)

	if d.Parameters == nil {
		d.Parameters = make(Documentation)
	}
	d.Parameters.Merge(from.Parameters)

}

func (d Documentation) ColumnExample(table, column string) (string, bool) {
	return d.ColumnDescription(table, column+"$example")
}

func (d Documentation) ColumnDescription(table, column string) (string, bool) {
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

func (d Documentation) Merge(output Documentation) {
	for key, value := range output {
		d[key] = value
	}
}
