package bootstrap

import (
	"fmt"
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
	output *xshape.Type
	lookup xshape.Lookup
}

func (c *artifactCompiler) compileOutputColumns(component *spec.Component) error {
	if component.RootView == nil || component.RootView.Columns != nil {
		return nil
	}
	fields, err := dtag.NewBindingIndex(c.input.OutputType)
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
		return (&outputColumnCompiler{output: xshape.Linked(c.input.OutputType)}).compile(component.RootView, field.Name)
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
	if field.Tag.Get("setMarker") == "true" || field.Tag.Get("internal") == "true" {
		return nil, true, nil
	}
	formatting, err := format.Parse(field.Tag, "json")
	if err != nil {
		return nil, false, err
	}
	if formatting.Ignore {
		return nil, true, nil
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
	}
	groupable := metadata.Groupable
	result := &spec.Column{
		Name: name, NameInferred: nameInferred, Source: source, Type: typeRef, Nullable: nullable,
		Groupable: &groupable, Tag: string(field.Tag), DatabaseType: strings.TrimSpace(sqlTag.DataType),
		PrimaryKey: sqlTag.PrimaryKey, AutoIncrement: sqlTag.Autoincrement, Unique: sqlTag.IsUnique,
	}
	if metadata.Codec != nil {
		result.Codec = &spec.Codec{Body: metadata.Codec.Body, Args: append([]string(nil), metadata.Codec.Arguments...), OutputType: metadata.Codec.OutputType}
	}
	return result, field.Anonymous, nil
}
