package tags

import (
	"embed"
	"github.com/viant/afs"
	"github.com/viant/tagly/format"
	"github.com/viant/tagly/tags"
	"github.com/viant/xreflect"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type (
	Tag struct {
		embed       *embed.FS
		fs          afs.Service
		View        *View
		SQL         ViewSQL
		SummarySQL  ViewSQLSummary
		Parameter   *Parameter
		LinkOn      LinkOn
		Predicates  []*Predicate
		Codec       *Codec
		Handler     *Handler
		TypeName    string
		Description string
		Example     string
		Value       *string
		Format      *format.Tag
	}
	Tagger interface {
		Tag() *tags.Tag
	}
)

func (t *Tag) GetValue(destType reflect.Type) (interface{}, error) {
	if t.Value == nil {
		return nil, nil
	}
	isPtr := false
	if destType.Kind() == reflect.Ptr {
		isPtr = true
		destType = destType.Elem()
	}

	isSlice := false

	if destType.Kind() == reflect.Slice {
		isSlice = true
		destType = destType.Elem()
	}

	switch destType.Kind() {
	case reflect.Bool:
		return strconv.ParseBool(*t.Value)
	case reflect.Int:
		return strconv.Atoi(*t.Value)
	case reflect.Float64:
		return strconv.ParseFloat(*t.Value, 64)
	default:
		if isPtr {
			return t.Value, nil
		}
		if isSlice && t.Value != nil {
			return strings.Split(*t.Value, ","), nil
		}
		return t.Value, nil
	}
}

func (t *Tag) UpdateTag(tag reflect.StructTag) reflect.StructTag {
	pTags := tags.NewTags(string(tag))

	var ret []*tags.Tag
	if t.Parameter != nil {
		t.appendTag(t.Parameter, &ret)
	}
	if t.Codec != nil {
		t.appendTag(t.Codec, &ret)
	}
	if t.Handler != nil {
		t.appendTag(t.Handler, &ret)
	}

	if t.Format != nil {
		if formatTag := tags.NewTag(format.TagName, t.Format).Values; formatTag != "" {
			pTags.Set(format.TagName, string(formatTag))
		}
	}
	if t.SQL.URI != "" {
		pTags.Set(SQLTag, "uri="+string(t.SQL.URI))
	} else if t.SQL.SQL != "" {
		pTags.Set(SQLTag, string(t.SQL.SQL))
	}
	if t.SummarySQL.URI != "" {
		pTags.Set(SQLSummaryTag, "uri="+string(t.SummarySQL.URI))
	} else if t.SummarySQL.SQL != "" {
		pTags.Set(SQLSummaryTag, string(t.SummarySQL.SQL))
	}

	if t.Description != "" {
		pTags.Set(DescriptionTag, string(t.Description))
	}
	if t.Example != "" {
		pTags.Set(ExampleTag, string(t.Example))
	}

	if t.TypeName != "" {
		pTags.Set(xreflect.TagTypeName, string(t.TypeName))
	}
	var rawTag string
	if t.View != nil {
		t.appendTag(t.View, &ret)
		if t.View.CustomTag != "" {
			rawTag = t.View.CustomTag
		}
	}
	t.appendTag(t.LinkOn, &ret)

	if t.Value != nil {
		pTags.Set(ValueTag, *t.Value)
	}

	for _, aTag := range ret {
		pTags.Set(aTag.Name, string(aTag.Values))
	}
	for _, aPredicate := range t.Predicates {
		aTag := aPredicate.Tag()
		pTags = append(pTags, aTag)
	}

	sort.Slice(pTags, func(i, j int) bool {
		prev := getTagPriority(pTags[i])
		next := getTagPriority(pTags[j])
		return prev > next
	})

	structTag := pTags.Stringify()
	if rawTag != "" {
		structTag = structTag + " " + rawTag
	}
	return reflect.StructTag(structTag)
}

func getTagPriority(tag *tags.Tag) int {
	switch tag.Name {
	case ParameterTag:
		return 100
	case CodecTag:
		return 99
	case PredicateTag:
		return 98
	case ValueTag:
		return 97
	case ViewTag:
		return 96
	case LinkOnTag:
		return 95
	case xreflect.TagTypeName:
		return 90
	default:
		return 0
	}
}

func (t *Tag) appendTag(tagger Tagger, tags *[]*tags.Tag) {
	if tagger != nil {
		if tagValue := tagger.Tag(); tagValue != nil {
			*tags = append(*tags, tagValue)
		}
	}
}

func (t *Tag) EnsurePredicate() *Predicate {
	if t == nil {
		return &Predicate{}
	}
	if len(t.Predicates) == 0 {
		t.Predicates = append(t.Predicates, &Predicate{})
	}
	return t.Predicates[0]
}

func (t *Tag) ensureView() *View {
	if t.View == nil {
		t.View = &View{}
	}
	return t.View
}
