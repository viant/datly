package json

import "strings"

type Tag struct {
	FieldName string
	OmitEmpty bool
}

const TagName = "json"

func Parse(tagValue string) *Tag {
	tag := &Tag{}

	segments := strings.Split(tagValue, ",")
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
