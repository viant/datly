package docs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	xdocs "github.com/viant/xdatly/docs"
	"gopkg.in/yaml.v3"
	"io/fs"
	"path"
	"sort"
	"strings"
)

type Asset struct {
	Path string
	Data []byte
}
type PackageRequest struct {
	Source    xdocs.Source
	Namespace string
}
type Package struct {
	Source xdocs.Source
	Assets []Asset
}

// Package prepares relocatable authored documents for the existing generated
// resource emitter. Schema bundling stays in the native schema owner. All source
// bytes still come from the same Bindly store, never a second filesystem map.
func (l Loader) Package(ctx context.Context, request PackageRequest) (*Package, error) {
	snapshot, err := l.Load(ctx, request.Source)
	if err != nil {
		return nil, err
	}
	result := &Package{Source: request.Source.Clone()}
	result.Source.BaseURL = ""
	result.Source.Substitutes = nil
	origins := snapshot.Origins()
	global := len(request.Source.GlobalURLs)
	result.Source.GlobalURLs = nil
	result.Source.DocURLs = nil
	result.Source.DocURL = ""
	for index, ref := range origins {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target, err := result.assetPath(ref)
		if err != nil {
			return nil, err
		}
		raw, err := l.Resources.ReadFile(ref)
		if err != nil {
			return nil, err
		}
		decoder := yaml.NewDecoder(strings.NewReader(request.Source.Expand(string(raw))))
		var node yaml.Node
		if err = decoder.Decode(&node); err != nil {
			return nil, err
		}
		root := node.Content[0]
		for i := 0; i < len(root.Content); i += 2 {
			if !strings.EqualFold(root.Content[i].Value, "Responses") {
				continue
			}
			var responses map[string]openapi3.Responses
			if err = root.Content[i+1].Decode(&responses); err != nil {
				return nil, err
			}
			paths := make([]string, 0, len(responses))
			for key := range responses {
				paths = append(paths, key)
			}
			sort.Strings(paths)
			for _, route := range paths {
				codes := make([]string, 0, len(responses[route]))
				for key := range responses[route] {
					codes = append(codes, key)
				}
				sort.Strings(codes)
				for _, code := range codes {
					response := responses[route][code]
					if response == nil {
						continue
					}
					for _, media := range response.Content {
						media.Schema, err = result.schema(l, request, ref, media.Schema)
						if err != nil {
							return nil, err
						}
					}
					for _, header := range response.Headers {
						header.Schema, err = result.schema(l, request, ref, header.Schema)
						if err != nil {
							return nil, err
						}
					}
				}
			}
			if err = root.Content[i+1].Encode(responses); err != nil {
				return nil, err
			}
		}
		var content bytes.Buffer
		encoder := yaml.NewEncoder(&content)
		encoder.SetIndent(2)
		if err = encoder.Encode(&node); err != nil {
			return nil, err
		}
		if err = encoder.Close(); err != nil {
			return nil, err
		}
		result.Assets = append(result.Assets, Asset{Path: target, Data: content.Bytes()})
		if index < global {
			result.Source.GlobalURLs = append(result.Source.GlobalURLs, request.Namespace+":"+target)
		} else {
			result.Source.DocURLs = append(result.Source.DocURLs, request.Namespace+":"+target)
		}
	}
	if len(request.Source.DocURLs) == 0 && request.Source.DocURL != "" && len(result.Source.DocURLs) == 1 {
		result.Source.DocURL = result.Source.DocURLs[0]
		result.Source.DocURLs = nil
	}
	return result, nil
}
func (p *Package) assetPath(ref string) (string, error) {
	namespace, file, ok := strings.Cut(ref, ":")
	if !ok {
		file = namespace
		namespace = "default"
	}
	if !fs.ValidPath(file) || !fs.ValidPath(namespace) || strings.Contains(namespace, "/") {
		return "", fmt.Errorf("invalid documentation resource %q", ref)
	}
	return path.Join("datly_docs", namespace, file), nil
}
func (p *Package) schema(loader Loader, request PackageRequest, ref string, source *openapi3.Schema) (*openapi3.Schema, error) {
	if source == nil {
		return nil, fmt.Errorf("authored schema required")
	}
	data, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var object map[string]any
	if err = json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	bundle, err := (jsonschema.ResourceCompiler{FS: loader.Resources}).Compile(jsonschema.ResourceRequest{Schema: object, Base: ref, Prefix: "#/definitions/"})
	if err != nil {
		return nil, err
	}
	// Closed schema resources remain authored static JSON Schema, independent of
	// Go entities and invocation bodies. Relative reference resolution happened once.
	root := bundle.Schema
	if len(bundle.Definitions) > 0 {
		root["definitions"] = bundle.Definitions
	}
	data, err = json.Marshal(root)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("datly_docs/schemas/%x.json", sha256.Sum256(data))
	p.Assets = append(p.Assets, Asset{Path: name, Data: data})
	for _, resource := range bundle.Resources {
		original, err := loader.Resources.ReadFile(resource)
		if err != nil {
			return nil, err
		}
		target, err := p.assetPath(resource)
		if err != nil {
			return nil, err
		}
		p.Assets = append(p.Assets, Asset{Path: target, Data: original})
	}
	return &openapi3.Schema{Ref: request.Namespace + ":" + name}, nil
}
