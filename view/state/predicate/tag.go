package predicate

import (
	"strconv"
	"strings"
)

var TagName = "predicate"

type (

	//Tag represents predicate tag
	Tag struct {
		Name        string
		Inclusion   bool
		Exclusion   bool
		IncludeName string
		ExcludeName string
		Group       int
		Predicate   string
		Args        []string
		noTag       bool
	}
)

func (t *Tag) initTagWithName(name string) {
	t.noTag = true
	toLower := strings.ToLower(name)
	if strings.HasSuffix(toLower, "excl") {
		t.Exclusion = true
		t.Name = name[:len(name)-4]
	} else if strings.HasSuffix(toLower, "exclusion") {
		t.Exclusion = true
		t.Name = name[:len(name)-9]
	} else if strings.HasSuffix(toLower, "incl") {
		t.Inclusion = true
		t.Name = name[:len(name)-4]
	} else if strings.HasSuffix(toLower, "inclusion") {
		t.Inclusion = true
		t.Name = name[:len(name)-9]
	}
	t.init(name)
}

func (t *Tag) init(name string) {
	if t.Name == "" {
		t.Name = name
	}
	if t.IncludeName == "" {
		t.IncludeName = "Include"
	}
	if t.ExcludeName == "" {
		t.ExcludeName = "Exclude"
	}

	if !t.Inclusion && !t.Exclusion {
		t.Inclusion = true
	}
}

// ParseTag parses predicate tag
func ParseTag(tag, name string) *Tag {
	ret := &Tag{}
	if tag == "" {
		ret.initTagWithName(name)
		return ret
	}

	elements := strings.Split(tag, ";")
	for _, element := range elements {
		pair := strings.Split(element, "=")
		switch strings.ToLower(pair[0]) {
		case "name":
			if len(pair) == 2 {
				ret.Name = pair[1]
			}
		case "exclusion":
			ret.Exclusion = true
		case "inclusion":
			ret.Inclusion = true
		case "exclusion_name":
			ret.ExcludeName = ""
		case "inclusion_name":
			ret.IncludeName = ""
		case "group":
			ret.Group, _ = strconv.Atoi(pair[1])
		case "predicate":
			ret.Predicate = pair[1]
		case "args":
			if len(pair) == 2 {
				ret.Args = strings.Split(pair[1], ",")
			}
		default:
			if len(pair) == 1 {
				ret.Name = pair[0]
			}
		}
	}
	ret.init(name)
	return ret
}
