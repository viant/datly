package scan

import (
	"context"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	afsembed "github.com/viant/afs/embed"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/componenttag"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	taglytags "github.com/viant/tagly/tags"
)

// StructScanner scans arbitrary struct types and extracts Datly-relevant tags.
type StructScanner struct{}

// New returns a Scanner implementation for shape facade.
func New() *StructScanner {
	return &StructScanner{}
}

// Scan implements shape.Scanner.
func (s *StructScanner) Scan(ctx context.Context, source *shape.Source, _ ...shape.ScanOption) (*shape.ScanResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, shape.ErrNilSource
	}
	source.EnsureTypeRegistry()

	root, err := resolveRootType(source)
	if err != nil {
		return nil, err
	}

	embedder := resolveEmbedder(source)
	baseDir := source.BaseDir()
	rootValue := resolveRootValue(source)
	result := &Result{
		RootType: root,
		EmbedFS:  embedder.EmbedFS(),
		ByPath:   map[string]*Field{},
	}

	if err = s.scanStruct(source, root, rootValue, "", nil, "", embedder, baseDir, result, map[reflect.Type]bool{}); err != nil {
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

func resolveRootValue(source *shape.Source) reflect.Value {
	if source == nil || source.Struct == nil {
		return reflect.Value{}
	}
	value := reflect.ValueOf(source.Struct)
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	return value
}

func (s *StructScanner) scanStruct(
	source *shape.Source,
	rType reflect.Type,
	rootValue reflect.Value,
	prefix string,
	indexPrefix []int,
	inheritedQuerySelector string,
	embedder *state.FSEmbedder,
	baseDir string,
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
			Path:          path,
			Name:          field.Name,
			Index:         combinedIndex,
			Type:          field.Type,
			QuerySelector: inheritedQuerySelector,
			Tag:           field.Tag,
			Anonymous:     field.Anonymous,
		}
		if querySelector := tags.ParseQuerySelector(field.Tag.Get(tags.QuerySelectorTag)); querySelector != "" {
			descriptor.QuerySelector = querySelector
		}

		fieldFS := parseFS(field.Tag, embedder.EmbedFS(), baseDir)
		if hasAny(field.Tag, tags.ViewTag, tags.SQLTag, tags.SQLSummaryTag, tags.LinkOnTag) {
			descriptor.ViewTypeName, descriptor.ViewDest = parseShapeViewHints(field.Tag)
			parsed, err := tags.ParseViewTags(field.Tag, fieldFS)
			if err != nil {
				return fmt.Errorf("shape scan: failed to parse view tags on %s: %w", path, err)
			}
			descriptor.HasViewTag = true
			descriptor.ViewTag = parsed
			result.ViewFields = append(result.ViewFields, descriptor)
		}

		if hasAny(field.Tag, tags.ParameterTag, tags.PredicateTag, tags.CodecTag, tags.HandlerTag) {
			parsed, err := tags.ParseStateTags(field.Tag, fieldFS)
			if err != nil {
				return fmt.Errorf("shape scan: failed to parse state tags on %s: %w", path, err)
			}
			descriptor.HasStateTag = true
			descriptor.StateTag = parsed
			result.StateFields = append(result.StateFields, descriptor)
		}

		if hasAny(field.Tag, componenttag.TagName) {
			parsed, err := componenttag.Parse(field.Tag)
			if err != nil {
				return fmt.Errorf("shape scan: failed to parse component tags on %s: %w", path, err)
			}
			descriptor.HasComponentTag = true
			descriptor.ComponentTag = parsed
			fieldValue := fieldValueByIndex(rootValue, combinedIndex)
			contract, err := resolveComponentContract(source, field.Type, fieldValue, parsed)
			if err != nil {
				return fmt.Errorf("shape scan: failed to resolve component contract on %s: %w", path, err)
			}
			if contract != nil {
				descriptor.ComponentInputType = contract.InputType
				descriptor.ComponentOutputType = contract.OutputType
				descriptor.ComponentInputName = contract.InputName
				descriptor.ComponentOutputName = contract.OutputName
				if err := s.scanComponentContracts(source, path, fieldValue, contract, baseDir, result, visited); err != nil {
					return err
				}
			}
			result.ComponentFields = append(result.ComponentFields, descriptor)
		}

		result.Fields = append(result.Fields, descriptor)
		result.ByPath[path] = descriptor

		nextType := nestedStructType(field.Type)
		if nextType != nil && shouldRecurseIntoField(field, descriptor, nextType) {
			if err := s.scanStruct(source, nextType, rootValue, path, combinedIndex, descriptor.QuerySelector, embedder, baseDir, result, visited); err != nil {
				return err
			}
		}
	}
	return nil
}

func nestedStructType(rType reflect.Type) reflect.Type {
	for rType != nil {
		switch rType.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Array:
			rType = rType.Elem()
		default:
			if rType.Kind() == reflect.Struct {
				return rType
			}
			return nil
		}
	}
	return nil
}

func shouldRecurseIntoField(field reflect.StructField, descriptor *Field, nextType reflect.Type) bool {
	if nextType == nil || nextType.Kind() != reflect.Struct {
		return false
	}
	if field.Anonymous {
		return !isStdlib(nextType.PkgPath())
	}
	if descriptor != nil && descriptor.HasViewTag {
		// Source-reconstructed and StructOf-based semantic view structs often have no package path.
		// They still need recursive scanning so nested relation views are preserved.
		return true
	}
	if descriptor != nil && strings.TrimSpace(descriptor.QuerySelector) != "" {
		return true
	}
	return false
}

func (s *StructScanner) scanComponentContracts(
	source *shape.Source,
	prefix string,
	fieldValue reflect.Value,
	contract *componentContract,
	baseDir string,
	result *Result,
	visited map[reflect.Type]bool,
) error {
	if contract == nil {
		return nil
	}
	if contract.InputType != nil {
		embedder := state.NewFSEmbedder(nil)
		embedder.SetType(contract.InputType)
		if err := s.scanStruct(source, contractInputRoot(contract.InputType), componentFieldValue(fieldValue, "Inout"), prefix+".Inout", nil, "", embedder, baseDir, result, visited); err != nil {
			return err
		}
	}
	if contract.OutputType != nil {
		embedder := state.NewFSEmbedder(nil)
		embedder.SetType(contract.OutputType)
		if err := s.scanStruct(source, contractInputRoot(contract.OutputType), componentFieldValue(fieldValue, "Output"), prefix+".Output", nil, "", embedder, baseDir, result, visited); err != nil {
			return err
		}
	}
	return nil
}

func contractInputRoot(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func fieldValueByIndex(rootValue reflect.Value, index []int) reflect.Value {
	if !rootValue.IsValid() || len(index) == 0 {
		return reflect.Value{}
	}
	current := rootValue
	for _, idx := range index {
		for current.IsValid() && current.Kind() == reflect.Ptr {
			if current.IsNil() {
				return reflect.Value{}
			}
			current = current.Elem()
		}
		if !current.IsValid() || current.Kind() != reflect.Struct || idx < 0 || idx >= current.NumField() {
			return reflect.Value{}
		}
		current = current.Field(idx)
	}
	return current
}

func hasAny(tag reflect.StructTag, names ...string) bool {
	for _, name := range names {
		if _, ok := tag.Lookup(name); ok {
			return true
		}
	}
	return false
}

func parseFS(tag reflect.StructTag, existing *embed.FS, baseDir string) *embed.FS {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return existing
	}
	uris := sqlURIs(tag)
	if len(uris) == 0 {
		return existing
	}
	holder := afsembed.NewHolder()
	if existing != nil {
		holder.AddFs(existing, ".")
	}
	added := 0
	for _, URI := range uris {
		if URI == "" || filepath.IsAbs(URI) || strings.Contains(URI, "://") {
			continue
		}
		absPath := filepath.Join(baseDir, filepath.FromSlash(URI))
		data, err := os.ReadFile(absPath)
		if err != nil {
			continue
		}
		holder.Add(filepath.ToSlash(URI), string(data))
		added++
	}
	if added == 0 {
		return existing
	}
	return holder.EmbedFs()
}

func sqlURIs(tag reflect.StructTag) []string {
	var result []string
	appendURI := func(tagName string) {
		value := strings.TrimSpace(tag.Get(tagName))
		if !strings.HasPrefix(value, "uri=") {
			return
		}
		URI := strings.TrimSpace(value[4:])
		if URI != "" {
			result = append(result, URI)
		}
	}
	appendURI(tags.SQLTag)
	appendURI(tags.SQLSummaryTag)
	return result
}

func isStdlib(pkg string) bool {
	if pkg == "" {
		return true
	}
	return !strings.Contains(pkg, ".")
}

func parseShapeViewHints(tag reflect.StructTag) (string, string) {
	raw, ok := tag.Lookup(tags.ViewTag)
	if !ok {
		return "", ""
	}
	_, values := taglytags.Values(raw).Name()
	var typeName, dest string
	_ = values.MatchPairs(func(key, value string) error {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "type":
			typeName = strings.TrimSpace(value)
		case "typename":
			typeName = strings.TrimSpace(value)
		case "dest":
			dest = strings.TrimSpace(value)
		}
		return nil
	})
	return typeName, dest
}
