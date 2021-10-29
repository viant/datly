package tag

import "strings"

//Parse parses tag
func Parse(tagString string) *Tag {
	elements := strings.Split(tagString, ",")
	tag := &Tag{}
	if tagString == "" {
		return tag
	}
	if tagString == "-" {
		tag.Transient = true
		return tag
	}

	for _, element := range elements {
		nv := strings.SplitN(element, "=", 2)
		if len(nv) == 2 {
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "name":
				tag.Name = strings.TrimSpace(nv[1])
			case "table":
				tag.Table = strings.TrimSpace(nv[1])
			case "sql":
				tag.SQL = strings.TrimSpace(nv[1])
			case "column":
				tag.Column = strings.TrimSpace(nv[1])
			case "on":
				tag.On = strings.TrimSpace(nv[1])

			}
			continue
		}
	}
	return tag
}
