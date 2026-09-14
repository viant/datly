package docs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"golang.org/x/net/http/httpguts"
	"gopkg.in/yaml.v3"
	"io/fs"
	"maps"
	"mime"
	"slices"
	"strconv"
	"strings"
)

// Responses returns a detached native OpenAPI response contract. It describes
// transport-ready outputs only, and is never inferred from a runtime payload.
func (s *Snapshot) Responses(path string) openapi3.Responses {
	if s == nil || s.responses[path] == "" {
		return nil
	}
	var result openapi3.Responses
	_ = json.Unmarshal([]byte(s.responses[path]), &result)
	return result
}
func (s *Snapshot) decodeResponses(node *yaml.Node, resources fs.FS, reference string) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("Responses requires route dictionary")
	}
	seen := map[string]bool{}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Tag != "!!str" || !strings.HasPrefix(key.Value, "/") || seen[key.Value] {
			return fmt.Errorf("invalid or duplicate response route %q", key.Value)
		}
		seen[key.Value] = true
		data, err := yaml.Marshal(value)
		if err != nil {
			return err
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		var responses openapi3.Responses
		if err = decoder.Decode(&responses); err != nil {
			return err
		}
		if len(responses) == 0 {
			return fmt.Errorf("response %s is empty", key.Value)
		}
		for _, status := range slices.Sorted(maps.Keys(responses)) {
			response := responses[status]
			code, err := strconv.Atoi(status)
			if status != "default" && (err != nil || code < 100 || code > 599) {
				return fmt.Errorf("invalid response status %q", status)
			}
			if response == nil || response.Description == nil || strings.TrimSpace(*response.Description) == "" {
				return fmt.Errorf("response %s requires description", status)
			}
			for _, media := range slices.Sorted(maps.Keys(response.Content)) {
				content := response.Content[media]
				if _, _, err := mime.ParseMediaType(media); err != nil {
					return err
				}
				if content == nil || content.Schema == nil {
					return fmt.Errorf("response media %s requires authored schema", media)
				}
				content.Schema, err = s.compileSchema(content.Schema, resources, reference)
				if err != nil {
					return fmt.Errorf("response %s %s: %w", status, media, err)
				}
			}
			for _, name := range slices.Sorted(maps.Keys(response.Headers)) {
				header := response.Headers[name]
				if !httpguts.ValidHeaderFieldName(name) || header == nil || header.Schema == nil {
					return fmt.Errorf("invalid response header %s", name)
				}
				header.Schema, err = s.compileSchema(header.Schema, resources, reference)
				if err != nil {
					return fmt.Errorf("response header %s: %w", name, err)
				}
			}
		}
		encoded, err := json.Marshal(responses)
		if err != nil {
			return err
		}
		s.responses[key.Value] = string(encoded)
	}
	return nil
}

func (s *Snapshot) compileSchema(source *openapi3.Schema, resources fs.FS, reference string) (*openapi3.Schema, error) {
	data, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var object map[string]any
	if err = json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	bundle, err := (jsonschema.ResourceCompiler{FS: resources}).Compile(jsonschema.ResourceRequest{Schema: object, Base: reference, Prefix: "#/components/schemas/"})
	if err != nil {
		return nil, err
	}
	for name, definition := range bundle.Definitions {
		encoded, err := json.Marshal(definition)
		if err != nil {
			return nil, err
		}
		if previous := s.schemas[name]; previous != "" && previous != string(encoded) {
			return nil, fmt.Errorf("conflicting authored schema identity %s", name)
		}
		s.schemas[name] = string(encoded)
	}
	s.schemaResources = append(s.schemaResources, bundle.Resources...)
	data, err = json.Marshal(bundle.Schema)
	if err != nil {
		return nil, err
	}
	var result openapi3.Schema
	if err = json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
func (s *Snapshot) Schemas() openapi3.Schemas {
	if s == nil {
		return nil
	}
	result := openapi3.Schemas{}
	for name, data := range s.schemas {
		var schema openapi3.Schema
		_ = json.Unmarshal([]byte(data), &schema)
		result[name] = &schema
	}
	return result
}
func (s *Snapshot) SchemaResources() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.schemaResources...)
}
