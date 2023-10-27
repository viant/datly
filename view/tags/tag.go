package tags

import (
	"embed"
	"github.com/viant/afs"
	"github.com/viant/structology/format"
	"github.com/viant/structology/tags"
	"github.com/viant/xreflect"
	"reflect"
)

type (
	Tag struct {
		embed        *embed.FS
		fs           afs.Service
		View         *View
		SQL          ViewSQL
		SummarySQL   ViewSQLSummary
		Parameter    *Parameter
		LinkOn       LinkOn
		Predicates   []*Predicate
		Codec        *Codec
		TypeName     string
		Description  string
		DefaultValue string
		Format       *format.Tag
	}
	Tagger interface {
		Tag() *tags.Tag
	}
)

func (t *Tag) UpdateTag(tag reflect.StructTag) reflect.StructTag {
	pTags := tags.NewTags(string(tag))
	var ret []*tags.Tag
	t.appendTag(t.View, &ret)
	t.appendTag(t.LinkOn, &ret)
	t.appendTag(t.Parameter, &ret)
	t.appendTag(t.Codec, &ret)

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
	if t.Description != "" {
		pTags.Set(DescriptionTag, string(t.Description))
	}
	if t.TypeName != "" {
		pTags.Set(xreflect.TagTypeName, string(t.TypeName))
	}
	if t.DefaultValue != "" {
		pTags.Set(DefaultValueTag, t.DefaultValue)
	}
	for _, aTag := range ret {
		pTags.Set(aTag.Name, string(aTag.Values))
	}
	for _, aPredicate := range t.Predicates {
		aTag := aPredicate.Tag()
		pTags = append(pTags, aTag)
	}

	return reflect.StructTag(pTags.Stringify())
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
