package tags

import (
	"fmt"
	"github.com/viant/tagly/tags"
	"strconv"
	"strings"
)

const ParameterTag = "parameter"

type Parameter struct {
	Name         string `tag:"name,omitempty"`
	Kind         string `tag:"kind,omitempty"`  //parameter location kind
	In           string `tag:"in,omitempty"`    //parameter location name
	When         string `tag:"when,omitempty"`  //condition to evaluate
	Scope        string `tag:"scope,omitempty"` //parameter scope
	ErrorCode    int    `tag:"errorCode,omitempty"`
	ErrorMessage string `tag:"errorMessage,omitempty"`
	DataType     string `tag:"dataType,omitempty"`    //parameter input type
	Cardinality  string `tag:"cardinality,omitempty"` //parameter input type
	With         string `tag:"with,omitempty"`        //optional auxiliary type name holding parameters
	Required     *bool  `tag:"required,omitempty"`
	Cacheable    *bool  `tag:"cacheable,omitempty"`
	Async        bool   `tag:"async,omitempty"`
	URI          string `tag:"uri,omitempty"` //parameter URI
}

func (t *Tag) updatedParameter(key string, value string) (err error) {
	tag := t.Parameter
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "value":
		tag.Name = strings.Trim(value, "' ")
	case "kind":
		tag.Kind = strings.TrimSpace(value)
	case "in":
		tag.In = strings.Trim(strings.TrimSpace(value), "{}")
	case "when":
		tag.When = strings.TrimSpace(value)
	case "cardinality":
		tag.Cardinality = strings.TrimSpace(value)
	case "cacheable":
		value := strings.TrimSpace(value) == "" || strings.ToLower(strings.TrimSpace(value)) == "true"
		tag.Cacheable = &value
	case "async":
		tag.Async = true
	case "scope":
		tag.Scope = strings.TrimSpace(value)
	case "errorcode":
		if tag.ErrorCode, err = strconv.Atoi(strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("invalid error code: %w", err)
		}
	case "errormessage":
		tag.ErrorMessage = strings.TrimSpace(value)
	case "datatype":
		tag.DataType = strings.TrimSpace(value)
	case "uri":
		tag.URI = strings.TrimSpace(value)
	case "with":
		tag.With = value
	case "required":
		v := true
		switch strings.TrimSpace(value) {
		case "false", "0":
			v = false
		case "true", "1":
			v = true
		}
		tag.Required = &v
	default:
		return fmt.Errorf("invalid paramerer tag key: '%s'", key)
	}
	return err
}

func (p *Parameter) Tag() *tags.Tag {
	builder := &strings.Builder{}
	builder.WriteString(p.Name)
	appendNonEmpty(builder, "kind", p.Kind)
	appendNonEmpty(builder, "in", p.In)
	appendNonEmpty(builder, "when", p.When)
	appendNonEmpty(builder, "uri", p.URI)

	if p.Cacheable != nil {
		value := "false"
		if *p.Cacheable {
			value = "true"
		}
		appendNonEmpty(builder, "cacheable", value)
	}

	if p.Cardinality == "One" {
		appendNonEmpty(builder, "cardinality", "One")
	}
	appendNonEmpty(builder, "with", p.With)
	appendNonEmpty(builder, "scope", p.Scope)
	appendNonEmpty(builder, "dataType", p.DataType)
	if p.ErrorCode != 0 {
		appendNonEmpty(builder, "errorCode", strconv.Itoa(p.ErrorCode))
	}
	appendNonEmpty(builder, "errorMessage", p.ErrorMessage)
	if p.Required != nil {
		appendNonEmpty(builder, "required", strconv.FormatBool(*p.Required))
	}
	return &tags.Tag{Name: ParameterTag, Values: tags.Values(builder.String())}
}
