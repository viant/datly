package json

import (
	"strings"
)

type Tag struct {
	FieldName string
	OmitEmpty bool
	Transient bool
}

type XTag struct {
	Tag
	Inline bool
}

const (
	TagName  = "json"
	XTagName = "jsonx"
)

func Parse(tagValue string) *Tag {
	tag := &Tag{}

	segments := strings.Split(tagValue, ",")
	if len(segments) == 1 && segments[0] == "-" {
		tag.Transient = true
		return tag
	}

	for i, segment := range segments {
		switch i {
		case 0:
			tag.FieldName = segment
		case 1:
			tag.OmitEmpty = segment == "omitempty"
		}
	}

	return tag
}

func ParseXTag(jsonTag, customTag string) *XTag {
	aTag := Parse(jsonTag)

	xTag := &XTag{
		Tag: *aTag,
	}

	tagSegments := strings.Split(customTag, ",")
	for _, segment := range tagSegments {
		if segment == "inline" {
			xTag.Inline = true
		}
	}

	return xTag
}
