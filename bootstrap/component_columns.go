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
	output    *xshape.Type
	lookup    xshape.Lookup
	discovery bool
}

func (c *artifactCompiler) compileOutputColumns(component *spec.Component) error {
	return CompileOutputColumns(component, c.input.OutputType)
}

// CompileOutputColumns resolves the existing linked row projection before SQL
// metadata discovery, as well as before runtime artifact compilation.
func CompileOutputColumns(component *spec.Component, outputType reflect.Type) error {
	return compileOutputColumns(component, outputType, false)
}

// DiscoveryOutputColumns retains linked SQL-mapped hidden fields as type
// authority for discovery. Their existing visibility tags remain authoritative.
func DiscoveryOutputColumns(component *spec.Component, outputType reflect.Type) ([]*spec.Column, error) {
	copy := component.Clone()
	if copy.RootView == nil {
		return nil, nil
	}
	copy.RootView.Columns = nil
	if err := compileOutputColumns(copy, outputType, true); err != nil {
		return nil, err
	}
	return copy.RootView.Columns, nil
}

func compileOutputColumns(component *spec.Component, outputType reflect.Type, discovery bool) error {
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
		return (&outputColumnCompiler{output: xshape.Linked(outputType), discovery: discovery}).compile(component.RootView, field.Name)
	}
	return nil
}

// CompileLinkedViewColumns projects a linked row contract without extending an
// already supplied canonical projection.
func CompileLinkedViewColumns(view *spec.View, rowType reflect.Type) error {
	return (&outputColumnCompiler{output: xshape.Linked(rowType)}).compile(view, "")
}

// DiscoveryLinkedViewColumns includes SQL-mapped hidden fields only as private
// discovery authority; they do not extend the view's public projection.
func DiscoveryLinkedViewColumns(view *spec.View, rowType reflect.Type) ([]*spec.Column, error) {
	copy := view.Clone()
	copy.Columns = nil
	if err := (&outputColumnCompiler{output: xshape.Linked(rowType), discovery: true}).compile(copy, ""); err != nil {
		return nil, err
	}
	return copy.Columns, nil
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
	metadata, err := dtag.ParseField(field.StructField())
	if err != nil {
		return nil, false, err
	}
	sqlTag := sqlxio.ParseTag(field.Tag)
	// Response visibility does not remove explicitly mapped SQL fields from the
	// scan/discovery contract. Preserve their declared types and JSON tags;
	// Unmapped hidden fields and excluded embedded holders stay outside the schema.
	if (formatting.Ignore || field.Tag.Get("internal") == "true") && sqlTag.Name() == "" && metadata.Source == "" {
		return nil, true, nil
	}
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
