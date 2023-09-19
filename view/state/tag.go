package state

import (
	"embed"
	"fmt"
	"strings"
)

const TagName = "parameter"

// Tag state tag
type (
	Tag struct {
		Kind      string //parameter location kind
		Name      string
		In        string //parameter location name
		Codec     string
		Selector  string
		CodecArgs []string
		BodyURI   string
		Body      string
		*embed.FS
	}
)

// ParseTag parses datly tag
func ParseTag(tagString string, fs *embed.FS) (*Tag, error) {
	tag := &Tag{FS: fs}
	elements := strings.Split(tagString, ",")
	if len(elements) == 0 {
		return tag, nil
	}
	for _, element := range elements {
		nv := strings.Split(element, "=")
		switch len(nv) {
		case 2:
			switch strings.ToLower(strings.TrimSpace(nv[0])) {
			case "name":
				tag.Name = strings.TrimSpace(nv[1])
			case "in":
				tag.In = strings.TrimSpace(nv[1])
			case "kind":
				tag.Kind = strings.TrimSpace(nv[1])
			case "body":
				tag.Body = strings.TrimSpace(nv[1])
			case "selector":
				tag.Selector = strings.TrimSpace(nv[1])
			case "bodyuri":
				if tag.FS != nil {
					data, err := tag.FS.ReadFile(strings.TrimSpace(nv[1]))
					if err != nil {
						return tag, nil
					}
					tag.Body = string(data)
				} else {
					return nil, fmt.Errorf("emboed option was empty for :%v", nv[1])
				}
			}
		}
	}
	return tag, nil
}
