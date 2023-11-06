package tags

import (
	"fmt"
	"github.com/viant/tagly/tags"
	"strconv"
	"strings"
)

const LinkOnTag = "on"

type (
	//LinkOn represents relation join tag
	LinkOn []string // [relfieldX:][ns.]relcolumnX[(includeFlag)]=[reffieldX:][ns.]refcolumnX [,[relfieldY:][ns.]relcolumnY]

	linkOption struct {
		relField, relColumn, refField, refColumn string
		includeColumn                            *bool
	}
	LinkOption func(o *linkOption)
)

func (l LinkOn) Tag() *tags.Tag {
	return &tags.Tag{Name: LinkOnTag, Values: tags.Values(strings.Join(l, ","))}
}

func WithRelLink(field, column string, include *bool) LinkOption {
	return func(o *linkOption) {
		o.relColumn = column
		o.relField = field
		o.includeColumn = include
	}
}

func WithRefLink(field, column string) LinkOption {
	return func(o *linkOption) {
		o.refColumn = column
		o.refField = field
	}
}

func (o linkOption) Stringify() string {
	builder := strings.Builder{}
	if o.relField != "" {
		builder.WriteString(o.relField)
		if o.relColumn != "" {
			builder.WriteString(":")
		}
		builder.WriteString(o.relColumn)
	} else if o.relColumn != "" {
		builder.WriteString(o.relColumn)
	}
	if o.includeColumn != nil {
		builder.WriteString("(" + strconv.FormatBool(*o.includeColumn) + ")")
	}
	builder.WriteString("=")
	if o.refField != "" {
		builder.WriteString(o.refField)
		if o.refColumn != "" {
			builder.WriteString(":")
		}
		builder.WriteString(o.refColumn)
	} else if o.refColumn != "" {
		builder.WriteString(o.refColumn)
	}
	return builder.String()
}

func newJoinOnOptions(opts []LinkOption) *linkOption {
	ret := &linkOption{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (l LinkOn) ForEach(dest func(relField, relColumn, refField, refColumn string, includeColumn *bool) error) error {
	for _, elem := range l {
		err := l.decodeLink(dest, elem)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l LinkOn) decodeLink(cb func(relField string, relColumn string, refField string, refColumn string, includeColumn *bool) error, elem string) error {
	pair := strings.Split(elem, "=")
	if len(pair) != 2 {
		return fmt.Errorf("invalid encoded link format, expected [rel link spec]=[ref link spec], but had: %s", elem)
	}
	relField, relColumn, incl, err := extractLink(pair[0])
	if err != nil {
		return err
	}
	refField, refColumn, _, err := extractLink(pair[1])
	if err != nil {
		return err
	}
	if err := cb(refField, relColumn, relField, refColumn, incl); err != nil {
		return err
	}
	return nil
}

func extractLink(encoded string) (string, string, *bool, error) {
	rel := strings.Split(encoded, ":")
	var field, column string
	var include *bool
	switch len(rel) {
	case 1:
		column = rel[0]
	case 2:
		field = rel[0]
		column = rel[1]
	default:
		return "", "", nil, fmt.Errorf("invalid link format: expected: '[fieldX:][ns.]column', but had: %s", encoded)
	}
	if index := strings.Index(column, `(true)`); index != -1 {
		includeFlag := true
		include = &includeFlag
		column = column[:index]
	}
	return field, column, include, nil
}

func (l LinkOn) Append(opts ...LinkOption) LinkOn {
	options := newJoinOnOptions(opts)
	return append(l, options.Stringify())
}
