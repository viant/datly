package bootstrap

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/tagly/format"
	xshape "github.com/viant/x/shape"
)

// outputColumnCompiler projects package row metadata at artifact compilation.
// Declaration resolution remains source authority for transcribe's ownership
// comparison; inferred columns must not change that authored contract.
type outputColumnCompiler struct {
	output          *xshape.Type
	lookup          xshape.Lookup
	discoveryHidden bool
}

func (c *artifactCompiler) compileOutputColumns(component *spec.Component) error {
	return CompileOutputColumns(component, c.input.OutputType)
}

// CompileOutputColumns resolves the existing linked row projection before SQL
// metadata discovery, as well as before runtime artifact compilation.
func CompileOutputColumns(component *spec.Component, outputType reflect.Type) error {
	if component.RootView == nil || component.RootView.Columns != nil {
		return nil
	}
	fields, err := dtag.NewBindingIndex(outputType)
	if err != nil {
		return err
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || param.Source.Kind != "output" || param.Source.Name != "view" {
			continue
		}
		field, ok, err := fields.Resolve(param)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("output view holder %s is missing", param.Name)
		}
		return (&outputColumnCompiler{output: xshape.Linked(outputType)}).compile(component.RootView, field.Name)
	}
	return nil
}

// compile follows original Datly repository/shape/load's schema
// inference boundary. A supplied list (including an explicit empty projection)
// remains authoritative; output-only fields must not extend that projection.
func (r *outputColumnCompiler) compile(view *spec.View, holder string) error {
	if view.Columns != nil {
		return nil
	}
	fields, err := r.output.FieldsAt(holder)
	if err != nil {
		return fmt.Errorf("view %s row fields: %w", view.Name, err)
	}
	columns := make([]*spec.Column, 0, len(fields))
	// Visibility is Datly projection policy over native field indexes, not a
	// second traversal of linked or synthetic Go types. Excluded embedded
	// holders also exclude their promoted descendants.
	var excluded [][]int
fieldsLoop:
	for _, field := range fields {
		for _, index := range excluded {
			if len(field.Index) > len(index) && slices.Equal(field.Index[:len(index)], index) {
				continue fieldsLoop
			}
		}
		for _, relation := range view.Relations {
			if relation != nil && equalMetadataName(relation.Holder, field.Name) {
				excluded = append(excluded, field.Index)
				continue fieldsLoop
			}
		}
		column, suppress, err := r.column(field, holder+"."+field.Name)
		if err != nil {
			return fmt.Errorf("view %s column %s: %w", view.Name, field.Name, err)
		}
		if suppress {
			excluded = append(excluded, field.Index)
		}
		if column != nil {
			columns = append(columns, column)
		}
	}
	view.Columns = columns
	return nil
}

func (r *outputColumnCompiler) column(field xshape.Field, path string) (*spec.Column, bool, error) {
	if !field.Exported {
		return nil, false, nil
	}
	if field.Tag.Get("setMarker") == "true" {
		return nil, true, nil
	}
	formatting, err := format.Parse(field.Tag, "json")
	if err != nil {
		return nil, false, err
	}
	hidden := formatting.Ignore || field.Tag.Get("internal") == "true"
	if hidden && !r.discoveryHidden {
		return nil, true, nil
	}
	if r.discoveryHidden && hidden {
		mapped := sqlxio.ParseTag(field.Tag)
		if mapped.Name() == "" || mapped.Transient || mapped.PresenceProvider {
			return nil, true, nil
		}
		typ := field.ReflectedType
		for typ != nil && typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ == nil {
			return nil, true, nil
		}
		switch typ.Kind() {
		case reflect.Slice:
			// SQL binary values are scalars; structural slices remain excluded.
			if typ != reflect.TypeFor[[]byte]() {
				return nil, true, nil
			}
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		default:
			if typ.PkgPath() != "time" || typ.Name() != "Time" {
				return nil, true, nil
			}
		}
	}
	metadata, err := dtag.ParseField(field.StructField())
	if err != nil {
		return nil, false, err
	}
	sqlTag := sqlxio.ParseTag(field.Tag)
	if sqlTag.Transient || sqlTag.PresenceProvider || metadata.View != nil || metadata.Self != nil || len(metadata.Relation) > 0 {
		return nil, true, nil
	}
	// Native Fields already enumerates promoted fields of embedded structs.
	// Named scalar embeddings, including pointers, remain ordinary columns.
	if field.Anonymous && sqlTag.Name() == "" && metadata.Source == "" && metadata.Codec == nil {
		resolved, err := r.output.ResolveField(path)
		if err != nil {
			return nil, false, err
		}
		structural, err := xshape.New(resolved.Descriptor, r.lookup).IsStruct()
		if err != nil {
			return nil, false, err
		}
		if structural {
			return nil, false, nil
		}
	}
	identity, err := field.CanonicalType()
	if err != nil {
		return nil, false, err
	}
	typeRef := spec.TypeRef{Name: identity}
	nullable := false
	ref, err := (xshape.Resolver{}).Reference(identity)
	if err == nil {
		nullable = len(ref.Wrappers) > 0 && ref.Wrappers[0].Kind == xshape.WrapperPointer
		if len(ref.Wrappers) == 0 {
			typeRef = spec.TypeRef{Package: ref.Qualifier, Name: ref.Name}
		} else if len(ref.Wrappers) == 1 && nullable {
			typeRef = spec.TypeRef{Package: ref.Qualifier, Name: ref.Name, Pointer: true}
		}
	}
	source := metadata.Source
	if sqlTag.Name() != "" {
		source = sqlTag.Name()
	}
	if source == "" {
		source = field.Name
	}
	name := field.Name
	nameInferred := true
	if metadata.SelectorAlias != "" {
		name = metadata.SelectorAlias
		nameInferred = false
	} else if _, projected, ok := strings.Cut(sqlTag.Column, "|"); ok && strings.TrimSpace(projected) != "" {
		// SQLX's dual column tag retains the physical source before the pipe
		// and the authored query projection after it. Keep both identities in
		// the immutable output plan; do not reparse tags during execution.
		name = strings.TrimSpace(projected)
		nameInferred = false
	}
	groupable := metadata.Groupable
	result := &spec.Column{
		Name: name, NameInferred: nameInferred, Source: source, Output: metadata.SQLOutput, Type: typeRef, Nullable: nullable,
		Groupable: &groupable, Tag: dtag.CanonicalFieldTag(string(field.Tag)), DatabaseType: strings.TrimSpace(sqlTag.DataType),
		PrimaryKey: sqlTag.PrimaryKey, AutoIncrement: sqlTag.Autoincrement, Unique: sqlTag.IsUnique,
	}
	if metadata.Codec != nil {
		result.Codec = &spec.Codec{Body: metadata.Codec.Body, Args: append([]string(nil), metadata.Codec.Arguments...), OutputType: metadata.Codec.OutputType}
	}
	return result, field.Anonymous, nil
}

// CompileOutputDiscoveryColumns returns only hidden, explicitly mapped linked
// row fields. Their types authorize metadata discovery, never public selectors
// or JSON projection. It does not modify the component or its public columns.
func CompileOutputDiscoveryColumns(component *spec.Component, outputType reflect.Type) ([]*spec.Column, error) {
	if component == nil || component.RootView == nil || outputType == nil {
		return nil, nil
	}
	fields, err := dtag.NewBindingIndex(outputType)
	if err != nil {
		return nil, err
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || param.Source.Kind != "output" || param.Source.Name != "view" {
			continue
		}
		field, ok, err := fields.Resolve(param)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("output view holder %s is missing", param.Name)
		}
		view := component.RootView.Clone()
		view.Columns = nil
		compiler := &outputColumnCompiler{output: xshape.Linked(outputType), discoveryHidden: true}
		if err := compiler.compile(view, field.Name); err != nil {
			return nil, err
		}
		var result []*spec.Column
		for _, column := range view.Columns {
			tag := reflect.StructTag(column.Tag)
			if tag.Get("internal") == "true" || tag.Get("json") == "-" {
				result = append(result, column)
			}
		}
		return result, nil
	}
	return nil, nil
}
