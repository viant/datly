package tags

import (
	"context"
	"fmt"
	"github.com/viant/tagly/tags"
	"strings"
)

// CodecTag codec tag
const CodecTag = "codec"

type Codec struct {
	Name      string   `tag:"name,omitempty"`
	Body      string   `tag:"body,omitempty"`
	Arguments []string `tag:"arguments,omitempty"`
	DataType  string   `tag:"dataType,omitempty"`
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
		data, err := t.fs.DownloadWithURL(context.Background(), strings.TrimSpace(URI), t.getOptions()...)
		if err != nil {
			return err
		}
		tag.Body = string(data)
	default:
		if value != "" {
			return fmt.Errorf("invalid argument %s", value)
		}
		tag.Arguments = append(tag.Arguments, key)
	}
	return err
}

func (p *Codec) Tag() *tags.Tag {
	builder := &strings.Builder{}
	if p == nil {
		return nil
	}
	builder.WriteString(p.Name)
	for _, arg := range p.Arguments {
		builder.WriteString(",")
		builder.WriteString(arg)
	}
	return &tags.Tag{Name: CodecTag, Values: tags.Values(builder.String())}
}
