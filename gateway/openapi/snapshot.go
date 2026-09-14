package openapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Snapshot owns both document encodings, produced before application publication.
// Export returns detached bytes; serving never regenerates schemas or reads types.
type Snapshot struct{ json, yaml []byte }

type ExportRequest struct {
	// Path selects an exact authored public path group. Empty selects aggregate.
	Path   string
	Format string
}

func NewSnapshot(ctx context.Context, request Request) (*Snapshot, error) {
	document, err := (Generator{}).Generate(ctx, request)
	if err != nil {
		return nil, err
	}
	result := &Snapshot{}
	result.json, err = json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	result.yaml, err = yaml.Marshal(document)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Snapshot) Export(format string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("OpenAPI snapshot is unavailable")
	}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json":
		return append([]byte(nil), s.json...), nil
	case "yaml", "yml":
		return append([]byte(nil), s.yaml...), nil
	default:
		return nil, fmt.Errorf("unsupported OpenAPI document format %q", format)
	}
}
