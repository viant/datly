package transfer

import (
	"strings"
)

const (
	TagName = "transfer"
)

type Tag struct {
	From      string
	AsXmlTab  bool
	AsJsonTab bool
}

// ParseTag parses datly tag
func ParseTag(tagString string) *Tag {
	elements := strings.Split(tagString, ",")
	ret := &Tag{}
	for _, element := range elements {
		nv := strings.Split(element, "=")
		switch len(nv) {
		case 1:
			ret.From = strings.TrimSpace(nv[0])
		case 2:
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "from":
				ret.From = strings.TrimSpace(nv[1])
			case "jsontab":
				ret.AsJsonTab = strings.TrimSpace(nv[1]) == "true"
			case "xmltab":
				ret.AsXmlTab = strings.TrimSpace(nv[1]) == "true"
			}
		}
	}
	return ret
}
