package gen

import "strings"

const DatlyTag = "datly"

type Tag struct {
	Kind string
	In   string
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
			case "in":
				tag.In = strings.TrimSpace(nv[1])
			case "kind":
				tag.Kind = strings.TrimSpace(nv[1])
			}
			continue
		}
	}
	return tag
}
