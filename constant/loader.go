package constant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/viant/afs"
	"gopkg.in/yaml.v3"
)

// Loader reads one flat YAML/JSON scalar mapping. Multiple-file merging is not
// supported. JSON numbers never pass through float64.
type Loader struct{ FS afs.Service }

func (l Loader) Load(ctx context.Context, location string) (*Values, error) {
	if ctx == nil {
		return nil, fmt.Errorf("constant file context is required")
	}
	if location == "" {
		return nil, nil
	}
	if l.FS == nil {
		l.FS = afs.New()
	}
	data, err := l.FS.DownloadWithURL(ctx, location)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read constant file failed")
	}
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("invalid constant file URL")
	}
	ext := strings.ToLower(filepath.Ext(u.Path))
	switch ext {
	case ".json":
		if !json.Valid(data) {
			return nil, fmt.Errorf("invalid JSON constant file")
		}
	case ".yaml", ".yml":
	default:
		return nil, fmt.Errorf("constant file must be YAML or JSON")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err = decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("invalid constant file")
	}
	var extra yaml.Node
	if decoder.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("constant file must contain one document")
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("constant file must contain a scalar mapping")
	}
	values := map[string]string{}
	node := document.Content[0]
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return nil, fmt.Errorf("constant keys must be strings")
		}
		if _, ok := values[key.Value]; ok {
			return nil, fmt.Errorf("duplicate constant %q", key.Value)
		}
		if value.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("constant %q must be a string, number or boolean", key.Value)
		}
		switch value.Tag {
		case "!!str", "!!int", "!!float", "!!bool":
		default:
			return nil, fmt.Errorf("constant %q has unsupported value type %s", key.Value, value.Tag)
		}
		if value.Tag == "!!float" {
			switch strings.ToLower(value.Value) {
			case ".nan", ".inf", "+.inf", "-.inf":
				return nil, fmt.Errorf("constant %q must be finite", key.Value)
			}
		}
		values[key.Value] = value.Value
	}
	return New(values)
}
