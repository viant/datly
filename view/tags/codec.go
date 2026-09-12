package tags

import (
	"strings"

	"github.com/viant/tagly/tags"
)

// CodecTag codec tag
const CodecTag = "codec"

type Codec struct {
	Name      string   `tag:"name,omitempty"`
	Body      string   `tag:"body,omitempty"`
	URI       string   `tag:"uri,omitempty"`
	Arguments []string `tag:"arguments,omitempty"`
}

func (t *Tag) updatedCodec(key string, value string) (err error) {
	tag := t.Codec
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "body":
		tag.Body = value
	case "uri":
		URI := strings.TrimSpace(value)
		data, err := loadContent(URI, t.fs, t.getOptions(), t.embed)
		if err != nil {
			return err
		}
		tag.Body = string(data)
	default:
		expr := key
		if value != "" {
			expr += " =" + value
		}
		tag.Arguments = append(tag.Arguments, expr)
	}
	return err
}

func (p *Codec) Tag() *tags.Tag {
	builder := &strings.Builder{}
	if p == nil {
		return nil
	}
	builder.WriteString(p.Name)
	if p.URI != "" {
		builder.WriteString(",uri=")
		builder.WriteString(p.URI)
	}
	for _, arg := range p.Arguments {
		builder.WriteString(",")
		builder.WriteString(arg)
	}
	return &tags.Tag{Name: CodecTag, Values: tags.Values(builder.String())}
}
