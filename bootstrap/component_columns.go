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
	"github.com/viant/x"
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
	return CompileOutputColumns(component, c.input.OutputType)
}

// CompileOutputColumns resolves the existing linked row projection before SQL
// metadata discovery, as well as before runtime artifact compilation.
func CompileOutputColumns(component *spec.Component, outputType reflect.Type) error {
	if component.ReaderGraph != nil {
		return nil
	}
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
		fieldPath := field.Name
		if holder != "" {
			fieldPath = holder + "." + field.Name
		}
		column, suppress, err := r.column(field, fieldPath)
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
	result := &spec.Column{
		Name: name, NameInferred: nameInferred, Source: source, Output: metadata.SQLOutput, Type: typeRef, Nullable: nullable,
		Tag: dtag.CanonicalFieldTag(string(field.Tag)), DatabaseType: strings.TrimSpace(sqlTag.DataType),
		PrimaryKey: sqlTag.PrimaryKey, AutoIncrement: sqlTag.Autoincrement, Unique: sqlTag.IsUnique,
	}
	if _, declared := field.Tag.Lookup(dtag.GroupableName); declared {
		groupable := metadata.Groupable
		result.Groupable = &groupable
	}
	if metadata.Codec != nil {
		result.Codec = &spec.Codec{Body: metadata.Codec.Body, Args: append([]string(nil), metadata.Codec.Arguments...), OutputType: metadata.Codec.OutputType}
	}
	return result, field.Anonymous, nil
}

// CompileReaderGraphColumns snapshots existing nil-projection inference before
// native metadata admission. Supplied projections and canonical edges stay owned.
func CompileReaderGraphColumns(component *spec.Component, input, output *x.Type, lookup xshape.Lookup) error {
	if component == nil || component.ReaderGraph != nil {
		return fmt.Errorf("reader graph normalization requires an unadmitted component")
	}
	seen := map[*spec.View]string{}
	active := map[*spec.View]bool{}
	var visit func(*spec.View, *x.Type) error
	visit = func(view *spec.View, row *x.Type) error {
		if view == nil || row == nil {
			return fmt.Errorf("reader graph exact row descriptor required")
		}
		if active[view] {
			return fmt.Errorf("reader graph cycle")
		}
		if prior, ok := seen[view]; ok {
			if prior != row.Key() {
				return fmt.Errorf("reader graph has conflicting row authority")
			}
			return nil
		}
		seen[view] = row.Key()
		active[view] = true
		defer delete(active, view)
		compiler := outputColumnCompiler{output: xshape.New(row, lookup), lookup: lookup}
		if err := compiler.compile(view, ""); err != nil {
			return err
		}
		for _, relation := range view.Relations {
			if relation == nil || relation.View == nil || relation.Kind == spec.RelationKindDerived {
				return fmt.Errorf("reader graph unsupported relation ownership")
			}
			field, err := compiler.output.ResolveField(relation.Holder)
			if err != nil {
				return err
			}
			if err = visit(relation.View, field.Descriptor); err != nil {
				return err
			}
		}
		return nil
	}
	rootFound := false
	independent := map[*spec.View]bool{}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil {
			continue
		}
		var view *spec.View
		var contract *x.Type
		if param.Source.Kind == "output" && param.Source.Name == "view" {
			if rootFound {
				return fmt.Errorf("reader graph ambiguous output holder")
			}
			rootFound = true
			view = component.RootView
			contract = output
		} else if param.Source.Kind == "view" {
			contract = input
			for _, candidate := range component.Views {
				if candidate.Name == param.Source.Name || candidate.Key.Name == param.Source.Name {
					if view != nil {
						return fmt.Errorf("reader graph ambiguous independent holder")
					}
					view = candidate
				}
			}
			if view != nil {
				independent[view] = true
			}
		} else {
			continue
		}
		if view == nil || contract == nil {
			return fmt.Errorf("reader graph exact holder required")
		}
		field, err := xshape.New(contract, lookup).ResolveField(param.Name)
		if err != nil {
			return err
		}
		if err = visit(view, field.Descriptor); err != nil {
			return err
		}
	}
	if !rootFound || len(independent) != len(component.Views) {
		return fmt.Errorf("reader graph missing exact view holders")
	}
	return nil
}
