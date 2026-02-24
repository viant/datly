package scan

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
)

// StructScanner scans arbitrary struct types and extracts Datly-relevant tags.
type StructScanner struct{}

// New returns a Scanner implementation for shape facade.
func New() *StructScanner {
	return &StructScanner{}
}

// Scan implements shape.Scanner.
func (s *StructScanner) Scan(_ context.Context, source *shape.Source, _ ...shape.ScanOption) (*shape.ScanResult, error) {
	if source == nil {
		return nil, shape.ErrNilSource
	}
	source.EnsureTypeRegistry()

	root, err := resolveRootType(source)
	if err != nil {
		return nil, err
	}

	embedder := resolveEmbedder(source)
	result := &Result{
		RootType: root,
		EmbedFS:  embedder.EmbedFS(),
		ByPath:   map[string]*Field{},
	}

	if err = s.scanStruct(root, "", nil, embedder, result, map[reflect.Type]bool{}); err != nil {
		return nil, err
	}

	return &shape.ScanResult{Source: source, Descriptors: result}, nil
}

func resolveRootType(source *shape.Source) (reflect.Type, error) {
	rType, err := source.ResolveRootType()
	if err != nil {
		return nil, err
	}
	if rType == nil {
		return nil, shape.ErrNilSource
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("shape scan: unsupported source type %v, expected struct", rType)
	}
	return rType, nil
}

func resolveEmbedder(source *shape.Source) *state.FSEmbedder {
	embedder := state.NewFSEmbedder(nil)
	if source.Type != nil {
		rType := source.Type
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
		embedder.SetType(rType)
		return embedder
	}
	if source.Struct != nil {
		rType := reflect.TypeOf(source.Struct)
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
		embedder.SetType(rType)
	}
	return embedder
}

func (s *StructScanner) scanStruct(
	rType reflect.Type,
	prefix string,
	indexPrefix []int,
	embedder *state.FSEmbedder,
	result *Result,
	visited map[reflect.Type]bool,
) error {
	if visited[rType] {
		return nil
	}
	visited[rType] = true
	defer delete(visited, rType)

	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		combinedIndex := append(append([]int{}, indexPrefix...), field.Index...)

		descriptor := &Field{
			Path:      path,
			Name:      field.Name,
			Index:     combinedIndex,
			Type:      field.Type,
			Tag:       field.Tag,
			Anonymous: field.Anonymous,
		}

		if hasAny(field.Tag, tags.ViewTag, tags.SQLTag, tags.SQLSummaryTag, tags.LinkOnTag) {
			parsed, err := tags.ParseViewTags(field.Tag, embedder.EmbedFS())
			if err != nil {
				return fmt.Errorf("shape scan: failed to parse view tags on %s: %w", path, err)
			}
			descriptor.HasViewTag = true
			descriptor.ViewTag = parsed
			result.ViewFields = append(result.ViewFields, descriptor)
		}

		if hasAny(field.Tag, tags.ParameterTag, tags.SQLTag, tags.PredicateTag, tags.CodecTag, tags.HandlerTag) {
			parsed, err := tags.ParseStateTags(field.Tag, embedder.EmbedFS())
			if err != nil {
				return fmt.Errorf("shape scan: failed to parse state tags on %s: %w", path, err)
			}
			descriptor.HasStateTag = true
			descriptor.StateTag = parsed
			result.StateFields = append(result.StateFields, descriptor)
		}

		result.Fields = append(result.Fields, descriptor)
		result.ByPath[path] = descriptor

		nextType := field.Type
		for nextType.Kind() == reflect.Ptr {
			nextType = nextType.Elem()
		}
		if field.Anonymous && nextType.Kind() == reflect.Struct && !isStdlib(nextType.PkgPath()) {
			if err := s.scanStruct(nextType, path, combinedIndex, embedder, result, visited); err != nil {
				return err
			}
		}
	}
	return nil
}

func hasAny(tag reflect.StructTag, names ...string) bool {
	for _, name := range names {
		if _, ok := tag.Lookup(name); ok {
			return true
		}
	}
	return false
}

func isStdlib(pkg string) bool {
	if pkg == "" {
		return true
	}
	return !strings.Contains(pkg, ".")
}
