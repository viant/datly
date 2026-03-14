package ir

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Document represents DQL internal representation independent of YAML rendering.
// Root carries the route/resource model as generic tree.
type Document struct {
	Root map[string]any
}

func FromYAML(data []byte) (*Document, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("dql ir: empty source")
	}
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	return &Document{Root: root}, nil
}
