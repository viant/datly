package view

import (
	"embed"
	"fmt"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
)

const (
	//DatlyTag represents datly tag
	DatlyTag = "datly"
)

type (
	//Tag state tag
	ParameterTag struct {
		Kind      string //parameter location kind
		In        string //parameter location name
		Codec     string
		CodecArgs []string
		BodyURI   string
		Body      string
	}

	RelationTag struct {
		RelColumn        string
		RelField         string
		RelIncludeColumn bool
		RefNamespace     string
		RefColumn        string
		RefField         string
		RefTable         string
		RefName          string
		RefConnector     string
		RefSQL           string
	}

	//Tag datly tag
	Tag struct {
		ParameterTag
		RelationTag
		*embed.FS
	}

	TagOption func(o *Tag)
)

// HasRelationSpec returns true if tag has relation
func (t *Tag) HasRelationSpec() bool {
	return (t.RefTable != "" || t.RefSQL != "") && (t.RefField != "" || t.RefColumn != "")
}

// RelationOption return tag relation option
func (t *Tag) RelationOption(field reflect.StructField, refViewOptions ...ViewOption) ViewOptions {
	var result []ViewOption
	if !t.HasRelationSpec() {
		return result
	}
	t.Init(field)
	if t.RefSQL != "" {
		refViewOptions = append(refViewOptions, WithTemplate(NewTemplate(t.RefSQL)))
	}
	var relOptions []RelationOption
	if t.RefNamespace != "" {
		relOptions = append(relOptions, WithRelationColumnNamespace(t.RefNamespace))
	}
	if t.RelField != "" {
		relOptions = append(relOptions, WithRelationField(t.RelField))
	}
	if t.RelIncludeColumn {
		relOptions = append(relOptions, WithRelationIncludeColumn(true))
	}
	if isSlice(field.Type) {
		result = append(result, WithOneToMany(field.Name, t.RelColumn,
			NwReferenceView(t.RefField, t.RefColumn,
				NewView(t.RefName, t.RefTable, refViewOptions...)), relOptions...))
		return result
	}
	result = append(result, WithOneToOne(field.Name, t.RelColumn,
		NwReferenceView(t.RefField, t.RefColumn,
			NewView(t.RefName, t.RefTable, refViewOptions...)), relOptions...))
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
	if t.RefSQL == "" {
		t.RefSQL, _ = field.Tag.Lookup("sql")
	}
}

// ParseTag parses datly tag
func ParseTag(tagString string, options ...TagOption) *Tag {
	tag := &Tag{}
	for _, opt := range options {
		opt(tag)
	}
	elements := strings.Split(tagString, ",")
	if len(elements) == 0 {
		return tag
	}
	for _, element := range elements {
		nv := strings.Split(element, "=")
		switch len(nv) {
		case 2:
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "in":
				tag.In = strings.TrimSpace(nv[1])
			case "kind":
				tag.Kind = strings.TrimSpace(nv[1])
			case "relcolumn":
				tag.RelColumn = strings.TrimSpace(nv[1])
			case "relfield":
				tag.RelField = strings.TrimSpace(nv[1])
			case "refcolumn":
				tag.RefColumn = strings.TrimSpace(nv[1])
			case "refns", "refnamespace":
				tag.RefNamespace = strings.TrimSpace(nv[1])
			case "reffield":
				tag.RefField = strings.TrimSpace(nv[1])
			case "reftable":
				tag.RefTable = strings.TrimSpace(nv[1])
			case "refname":
				tag.RefName = strings.TrimSpace(nv[1])
			case "refsql":
				tag.RefSQL = strings.TrimSpace(nv[1])
			case "sqluri":
				tag.RefSQL = strings.TrimSpace(nv[1])
			case "refconnector":
				tag.RefConnector = strings.TrimSpace(nv[1])
			}
			continue
		}
	}
	return tag
}

func generateFieldTag(column *Column, viewCaseFormat format.Case) string {
	columnName := column.Name
	defaultTag := createDefaultTagIfNeeded(column)
	sqlxTag := `sqlx:"name=` + columnName + `"`
	var aTag string
	if defaultTag == "" {
		aTag = sqlxTag
	} else {
		aTag = sqlxTag + " " + defaultTag
	}

	if column.Tag != "" {
		if aTag != "" {
			aTag += " "
		}
		aTag += column.Tag
	}
	if !strings.Contains(aTag, "velty") {
		names := columnName
		if aFieldName := state.StructFieldName(viewCaseFormat, columnName); aFieldName != names {
			names = names + "|" + aFieldName
		}
		aTag += fmt.Sprintf(` velty:"names=%v"`, names)
	}
	return aTag
}

func createDefaultTagIfNeeded(column *Column) string {
	if column == nil {
		return ""
	}
	attributes := make([]string, 0)
	if column.Format != "" {
		attributes = append(attributes, json.FormatAttribute+"="+column.Format)
	}
	if column.IgnoreCaseFormatter {
		attributes = append(attributes, json.IgnoreCaseFormatter+"=true,name="+column.Name)
	}
	if column.Default != "" {
		attributes = append(attributes, json.ValueAttribute+"="+column.Default)
	}
	if len(attributes) == 0 {
		return ""
	}
	return json.DefaultTagName + `:"` + strings.Join(attributes, ",") + `"`
}
