package tags

import (
	"fmt"
	"github.com/viant/structology/tags"
	"strconv"
	"strings"
)

// PredicateTag Predicate tag
const PredicateTag = "predicate"

type Predicate struct {
	Name        string
	Group       int
	Inclusion   bool
	Exclusion   bool
	IncludeName string
	ExcludeName string
	Filter      string
	Arguments   []string
}

func (p *Predicate) Init(name string) {
	lcName := strings.ToLower(name)
	if strings.HasSuffix(lcName, "excl") {
		p.Exclusion = true
		p.Filter = name[:len(name)-4]
	} else if strings.HasSuffix(lcName, "exclusion") {
		p.Exclusion = true
		p.Filter = name[:len(name)-9]
	} else if strings.HasSuffix(lcName, "incl") {
		p.Inclusion = true
		p.Filter = name[:len(name)-4]
	} else if strings.HasSuffix(lcName, "inclusion") {
		p.Inclusion = true
		p.Filter = name[:len(name)-9]
	}
	if p.Filter == "" {
		p.Filter = name
	}
	if p.IncludeName == "" {
		p.IncludeName = "Include"
	}
	if p.ExcludeName == "" {
		p.ExcludeName = "Exclude"
	}
	if !p.Inclusion && !p.Exclusion {
		p.Inclusion = true
	}
}

func (t *Tag) updatedPredicate(key string, value string) (err error) {
	tag := t.Predicates[len(t.Predicates)-1]
	lKey := strings.ToLower(key)
	switch lKey {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "group":
		if tag.Group, err = strconv.Atoi(strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("invalid predicate group: %s %w", value, err)
		}
	default:
		if value != "" {
			return fmt.Errorf("invalid argument %s", value)
		}
		tag.Arguments = append(tag.Arguments, key)
	}
	return err
}

func (p *Predicate) Tag() *tags.Tag {
	builder := &strings.Builder{}
	builder.WriteString(p.Name)
	builder.WriteString(",")
	builder.WriteString("group=")
	builder.WriteString(strconv.Itoa(p.Group))
	appendNonEmpty(builder, "filter", p.Filter)
	for _, arg := range p.Arguments {
		builder.WriteString(",")
		builder.WriteString(arg)
	}
	return &tags.Tag{Name: PredicateTag, Values: tags.Values(builder.String())}
}
