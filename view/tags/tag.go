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
)

type (
	Tag struct {
		embed         *embed.FS
		fs            afs.Service
		View          *View
		SQL           ViewSQL
		SummarySQL    ViewSQLSummary
		Parameter     *Parameter
		LinkOn        LinkOn
		Predicates    []*Predicate
		Codec         *Codec
		TypeName      string
		Documentation string
		Value         *string
		Format        *format.Tag
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
	if t.Format != nil {
		if formatTag := tags.NewTag(format.TagName, t.Format).Values; formatTag != "" {
			pTags.Set(format.TagName, string(formatTag))
		}
	}
	if t.SQL != "" {
		pTags.Set(SQLTag, string(t.SQL))
	}
	if t.SummarySQL != "" {
		pTags.Set(SQLSummaryTag, string(t.SummarySQL))
	}
	if t.Documentation != "" {
		pTags.Set(DocumentationTag, string(t.Documentation))
	}

	if t.TypeName != "" {
		pTags.Set(xreflect.TagTypeName, string(t.TypeName))
	}
	if t.View != nil {
		t.appendTag(t.View, &ret)
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

	return reflect.StructTag(pTags.Stringify())
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
