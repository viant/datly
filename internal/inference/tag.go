package inference

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io"
	"reflect"
	"strconv"
	"strings"
)

type (
	TagValue []string
	Tags     struct {
		tags  map[string]TagValue
		order []string
	}
)

//Append appends tag value element
func (e *TagValue) Append(element string) {
	if element == "" {
		return
	}
	*e = append(*e, element)
}

//Set sets tag value
func (t *Tags) Set(tag string, value TagValue) {
	if len(value) == 0 {
		return
	}
	if len(t.tags) == 0 {
		t.tags = map[string]TagValue{}
	}
	t.order = append(t.order, tag)
	t.tags[tag] = value
}

func (t *Tags) Init(tag string) {
	for tag != "" {
		i := 0
		for i < len(tag) && tag[i] == ' ' {
			i++
		}
		tag = tag[i:]
		if tag == "" {
			break
		}
		i = 0
		for i < len(tag) && tag[i] > ' ' && tag[i] != ':' && tag[i] != '"' && tag[i] != 0x7f {
			i++
		}
		if i == 0 || i+1 >= len(tag) || tag[i] != ':' || tag[i+1] != '"' {
			break
		}
		name := tag[:i]
		tag = tag[i+1:]
		i = 1
		for i < len(tag) && tag[i] != '"' {
			if tag[i] == '\\' {
				i++
			}
			i++
		}
		if i >= len(tag) {
			break
		}
		quotedValue := tag[:i+1]
		tag = tag[i+1:]
		value, err := strconv.Unquote(quotedValue)
		if err != nil {
			break
		}
		t.Set(name, strings.Split(value, ","))
	}
}

func (t *Tags) buildSqlxTag(source *Spec, field *Field) {
	column := field.Column
	tagValue := TagValue{}
	tagValue.Append("name=" + column.Name)
	if column.IsAutoincrement {
		tagValue.Append("autoincrement")
	}
	key := strings.ToLower(column.Name)
	if _, ok := source.pk[key]; ok {
		tagValue.Append("primaryKey")
	} else if fk, ok := source.Fk[key]; ok {
		tagValue.Append("refTable=" + fk.ReferenceTable)
		tagValue.Append("refColumn=" + fk.ReferenceColumn)
	} else if column.IsUnique {
		tagValue.Append("unique")
		tagValue.Append("table=" + source.Table)
	}
	field.Tags.Set("sqlx", tagValue)
}

func (t *Tags) buildJSONTag(field *Field) {
	tagValue := TagValue{}
	if field.Column.IsNullable {
		tagValue.Append(",omitempty")
	}
	t.Set("json", tagValue)
}

var standardValidations = []string{"email", "phone", "domain", "iabCategories", "ssn"}

func (t *Tags) buildValidateTag(field *Field) {
	tagValue := TagValue{}
	name := strings.ToLower(field.Name)

	if field.Column.IsNullable {
		tagValue.Append("omitempty")
	} else if !field.Column.IsAutoincrement {
		tagValue.Append("required")
	}

	for i := range standardValidations {
		validation := strings.ToLower(standardValidations[i])
		if strings.Contains(name, validation) {
			tagValue.Append(standardValidations[i])
		}
	}
	column := field.Column
	if column.Length != nil && *column.Length > 0 {
		tagValue.Append(fmt.Sprintf("le(%d)", *column.Length))
	}
	if len(tagValue) == 1 && tagValue[0] == "omitempty" {
		return
	}
	t.Set("validate", tagValue)
}

func (t *Tags) buildRelation(spec *Spec, relation *Relation) {
	join := relation.Join
	if join == nil || relation.KeyField == nil || relation.ParentField == nil {
		return
	}
	datlyTag := TagValue{}
	datlyTag.Append(fmt.Sprintf("relName=%s", join.Alias))
	datlyTag.Append(fmt.Sprintf("relColumn=%s", relation.ParentField.Column.Name))
	datlyTag.Append(fmt.Sprintf("relField=%s", relation.ParentField.Name))
	if spec.Table != "" {
		datlyTag.Append(fmt.Sprintf("refTable=%v", spec.Table))
	}
	datlyTag.Append(fmt.Sprintf("refColumn=%s", relation.KeyField.Column.Name))
	datlyTag.Append(fmt.Sprintf("refField=%s", relation.KeyField.Name))
	if relation.KeyField.Column.Namespace != "" {
		datlyTag.Append(fmt.Sprintf("refns=%s", relation.KeyField.Column.Namespace))
	}
	sqlTag := TagValue{}
	if rawSQL := strings.Trim(sqlparser.Stringify(join.With), " )("); rawSQL != "" {
		rawSQL = strings.Replace(rawSQL, "("+spec.Table+")", spec.Table, 1)
		sqlTag.Append(strings.ReplaceAll(rawSQL, "\n", " "))
	}
	t.Set("datly", datlyTag)
	t.Set("sql", sqlTag)
}

//Stringify return text representation of struct tag
func (t *Tags) Stringify() string {
	if len(t.order) == 0 {
		return ""
	}
	builder := strings.Builder{}
	//builder.WriteByte('`')
	for i, key := range t.order {
		value := t.tags[key]
		if i > 0 {
			builder.WriteString(" ")
		}
		builder.WriteString(key)
		builder.WriteString(":")
		tagValue := strconv.Quote(strings.Join(value, ","))
		builder.WriteString(tagValue)
	}
	//builder.WriteByte('`')
	return builder.String()
}

func DatlyTag(tag reflect.StructTag) *view.Tag {
	datlyTagString, _ := tag.Lookup("datly")
	if datlyTagString == "" {
		return nil
	}
	return view.ParseTag(datlyTagString)
}

func SqlxTag(tag reflect.StructTag) *io.Tag {
	datlyTagString, _ := tag.Lookup("sqlx")
	if datlyTagString == "" {
		return nil
	}
	return io.ParseTag(datlyTagString)
}