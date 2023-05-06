package view

import (
	"reflect"
	"strings"
)

const (
	//DatlyTag represents datly tag
	DatlyTag = "datly"
)

//Tag datly tag
type Tag struct {
	RelColumn    string
	RefColumn    string
	RefField     string
	RefTable     string
	RefName      string
	RefConnector string
	RefSQL       string
}

//HasRelationSpec returns true if tag has relation
func (t *Tag) HasRelationSpec() bool {
	return (t.RefTable != "" || t.RefSQL != "") && (t.RefField != "" || t.RefColumn != "")
}

//RelationOption return tag relation option
func (t *Tag) RelationOption(field reflect.StructField, refViewOptions ...ViewOption) ViewOptions {
	var result []ViewOption
	if !t.HasRelationSpec() {
		return result
	}
	t.Init(field)
	if t.RefSQL != "" {
		refViewOptions = append(refViewOptions, WithSQL(t.RefSQL))
	}
	if isSlice(field.Type) {
		result = append(result, WithOneToMany(field.Name, t.RelColumn,
			NwReferenceView(t.RefField, t.RefColumn,
				NewView(t.RefName, t.RefTable, refViewOptions...))))
		return result
	}
	result = append(result, WithOneToOne(field.Name, t.RelColumn,
		NwReferenceView(t.RefField, t.RefColumn,
			NewView(t.RefName, t.RefTable, refViewOptions...))))
	return result
}

func (t *Tag) Init(field reflect.StructField) {
	typeName, ok := field.Tag.Lookup("typeName")
	if !ok || typeName == "" {
		typeName = field.Name
	}
	if t.RefName == "" {
		t.RefName = typeName
	}
	if t.RelColumn == "" {
		t.RelColumn = field.Name
	}
	if t.RefField == "" {
		t.RefField = t.RefColumn
	}
	if t.RefColumn == "" {
		t.RefColumn = t.RefField
	}
}

//ParseTag parses datly tag
func ParseTag(tagString string) *Tag {
	tag := &Tag{}
	elements := strings.Split(tagString, ",")
	if len(elements) == 0 {
		return tag
	}
	for _, element := range elements {
		nv := strings.Split(element, "=")
		switch len(nv) {
		case 2:
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "relcolumn":
				tag.RelColumn = strings.TrimSpace(nv[1])
			case "refcolumn":
				tag.RefColumn = strings.TrimSpace(nv[1])
			case "reffield":
				tag.RefField = strings.TrimSpace(nv[1])
			case "reftable":
				tag.RefTable = strings.TrimSpace(nv[1])
			case "refname":
				tag.RefName = strings.TrimSpace(nv[1])
			case "refsql":
				tag.RefSQL = strings.TrimSpace(nv[1])
			case "refconnector":
				tag.RefConnector = strings.TrimSpace(nv[1])
			}
			continue
		}
	}
	return tag
}
