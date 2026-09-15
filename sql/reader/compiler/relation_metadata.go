package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/tagly/format/text"
	xshape "github.com/viant/x/shape"
)

type viewDeriver struct {
	rowTypes map[*data.View]reflect.Type
	active   map[reflect.Type]string
}

func newViewDeriver(rowTypes map[*data.View]reflect.Type) *viewDeriver {
	return &viewDeriver{rowTypes: rowTypes, active: map[reflect.Type]string{}}
}

func (d *viewDeriver) enrich(view *data.View, outputType reflect.Type) error {
	if view == nil || outputType == nil || outputType.Kind() != reflect.Struct {
		return nil
	}
	if origin, ok := d.active[outputType]; ok {
		return fmt.Errorf("recursive relation type %s at view %s re-enters view %s; model recursive rows with a self tag", outputType, view.Spec.Name, origin)
	}
	d.active[outputType] = view.Spec.Name
	defer delete(d.active, outputType)

	d.rowTypes[view] = outputType
	if view.CaseFormat == text.CaseFormatUndefined {
		view.CaseFormat = text.CaseFormatLowerCamel
	}
	columns, err := columnsFromType(outputType)
	if err != nil {
		return err
	}
	view.Columns = reconcileColumns(columns, view.Columns)
	selfReference, err := selfReferenceFromType(outputType)
	if err != nil {
		return err
	}
	if selfReference != nil {
		view.Spec.SelfReference = selfReference
	}
	if err := d.enrichPlannedRelations(view, outputType); err != nil {
		return err
	}
	for _, field := range reflect.VisibleFields(outputType) {
		if !field.IsExported() {
			continue
		}
		metadata, err := dtag.ParseField(field)
		if err != nil {
			return fmt.Errorf("parse field metadata for %s: %w", field.Name, err)
		}
		if len(metadata.Relation) == 0 {
			continue
		}
		if hasRelationHolder(view, field.Name) {
			continue
		}
		relation, err := d.relationFromField(view, outputType, field, metadata)
		if err != nil {
			return fmt.Errorf("plan relation %s: %w", field.Name, err)
		}
		view.Relations = append(view.Relations, relation)
	}
	// DQL can declare relation holders without repeating relation tags on Go
	// fields. Once those holders are resolved, they are graph edges, not SQL
	// columns of the parent view.
	physical := view.Columns[:0]
	for _, column := range view.Columns {
		if column != nil && hasRelationHolder(view, column.Name) {
			fieldTag := reflect.StructTag(column.Tag)
			scalar := sqlxio.ParseTag(fieldTag)
			if column.Codec != nil || fieldTag.Get(dtag.SourceName) != "" || (!scalar.Transient && (scalar.Column != "" || scalar.Encoding != "")) {
				return fmt.Errorf("relation holder %s also declares a scalar SQL or codec mapping", column.Name)
			}
			continue
		}
		physical = append(physical, column)
	}
	view.Columns = physical
	return nil
}

func (d *viewDeriver) enrichPlannedRelations(view *data.View, parentType reflect.Type) error {
	for _, relation := range view.Relations {
		if relation == nil || relation.Of == nil || relation.Of.View == nil {
			return fmt.Errorf("view %s contains an incomplete relation", view.Spec.Name)
		}
		holder, ok := typecatalog.FieldByName(parentType, relation.Holder)
		if !ok {
			return fmt.Errorf("relation %s holder %q was not found on %s", relation.Name, relation.Holder, parentType)
		}
		childType, cardinality, err := relationFieldType(holder.Type)
		if err != nil {
			return fmt.Errorf("relation %s holder %s: %w", relation.Name, holder.Name, err)
		}
		normalizedCardinality, err := spec.NormalizeCardinality(relation.Cardinality)
		if err != nil {
			return fmt.Errorf("relation %s: %w", relation.Name, err)
		}
		if normalizedCardinality != "" && normalizedCardinality != cardinality {
			return fmt.Errorf("relation %s cardinality %s conflicts with holder %s", relation.Name, relation.Cardinality, holder.Type)
		}
		relation.Holder = holder.Name
		relation.Cardinality = cardinality
		if err := resolveRelationFields(relation.On, parentType); err != nil {
			return fmt.Errorf("relation %s parent links: %w", relation.Name, err)
		}
		if err := resolveRelationFields(relation.Of.On, childType); err != nil {
			return fmt.Errorf("relation %s child links: %w", relation.Name, err)
		}
		if err := d.enrich(relation.Of.View, childType); err != nil {
			return err
		}
	}
	return nil
}

func (d *viewDeriver) enrichOutputRelations(relations []*data.Relation, outputType reflect.Type) error {
	for outputType != nil && outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}
	if len(relations) == 0 {
		return nil
	}
	if outputType == nil || outputType.Kind() != reflect.Struct {
		return fmt.Errorf("output type is required for output relations")
	}
	for _, relation := range relations {
		if relation == nil || relation.Of == nil || relation.Of.View == nil {
			return fmt.Errorf("output relation metadata is incomplete")
		}
		path := strings.TrimSpace(relation.Holder)
		holder, err := xshape.Linked(outputType).Accessor(path)
		if err != nil {
			field, ok := typecatalog.FieldByName(outputType, path)
			if !ok {
				return fmt.Errorf("output relation %s holder %q was not found on %s", relation.Name, relation.Holder, outputType)
			}
			path = field.Name
			holder, err = xshape.Linked(outputType).Accessor(path)
			if err != nil {
				return err
			}
		}
		childType, cardinality, err := relationFieldType(holder.Type())
		if err != nil {
			return fmt.Errorf("output relation %s holder %s: %w", relation.Name, path, err)
		}
		normalizedCardinality, err := spec.NormalizeCardinality(relation.Cardinality)
		if err != nil {
			return fmt.Errorf("output relation %s: %w", relation.Name, err)
		}
		if normalizedCardinality != "" && normalizedCardinality != cardinality {
			return fmt.Errorf("output relation %s cardinality %s conflicts with holder %s", relation.Name, relation.Cardinality, holder.Type())
		}
		relation.Holder = path
		relation.Cardinality = cardinality
		if err := d.enrich(relation.Of.View, childType); err != nil {
			return err
		}
	}
	return nil
}

func resolveRelationFields(links data.Links, rowType reflect.Type) error {
	columns, err := columnsFromType(rowType)
	if err != nil {
		return err
	}
	for _, link := range links {
		if link == nil {
			continue
		}
		for _, column := range columns {
			if column != nil && (strings.EqualFold(column.Column, link.Column) || strings.EqualFold(column.Name, link.Column)) {
				link.Field = column.Name
				break
			}
		}
		if link.Field == "" {
			return fmt.Errorf("column %q was not found on %s", link.Column, rowType)
		}
	}
	return nil
}

func hasRelationHolder(view *data.View, name string) bool {
	for _, relation := range view.Relations {
		if relation != nil && strings.EqualFold(relation.Holder, name) {
			return true
		}
	}
	return false
}

func selfReferenceFromType(rowType reflect.Type) (*spec.SelfReference, error) {
	var result *spec.SelfReference
	for _, field := range reflect.VisibleFields(rowType) {
		if !field.IsExported() {
			continue
		}
		metadata, err := dtag.ParseField(field)
		if err != nil {
			return nil, fmt.Errorf("parse field metadata for %s.%s: %w", rowType, field.Name, err)
		}
		if metadata.Self == nil {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("type %s declares more than one self-reference holder", rowType)
		}
		result = &spec.SelfReference{Holder: field.Name, Child: metadata.Self.Child, Parent: metadata.Self.Parent}
	}
	return result, nil
}

func (d *viewDeriver) relationFromField(parent *data.View, parentType reflect.Type, field reflect.StructField, metadata *dtag.Field) (*data.Relation, error) {
	childType, cardinality, err := relationFieldType(field.Type)
	if err != nil {
		return nil, err
	}
	columns, err := columnsFromType(childType)
	if err != nil {
		return nil, err
	}
	childName := field.Name
	if metadata.View != nil && metadata.View.Name != "" {
		childName = metadata.View.Name
	}
	child := &data.View{Spec: spec.View{Name: childName}, Relations: []*data.Relation{}, CaseFormat: parent.CaseFormat, Columns: columns}
	applyFieldViewTag(child, metadata.View)
	applyFieldSQLTag(child, metadata.SQL)
	if err := d.enrich(child, childType); err != nil {
		return nil, err
	}
	links, err := parseLinkOn(metadata.Relation, parentType, childType)
	if err != nil {
		return nil, err
	}
	relation := &data.Relation{
		Name: field.Name, Kind: spec.RelationKindSubview,
		Holder:      field.Name,
		Cardinality: cardinality, On: make(data.Links, 0, len(links)),
		Of: &data.RelationRef{View: child, On: make(data.Links, 0, len(links)), MatchStrategy: parseFieldMatchStrategy(viewMatch(metadata.View))},
	}
	for _, link := range links {
		relation.On = append(relation.On, &data.Link{Namespace: link.parentNamespace, Column: link.parentColumn, Field: link.parentField})
		relation.Of.On = append(relation.Of.On, &data.Link{Namespace: link.childNamespace, Column: link.childColumn, Field: link.childField})
	}
	return relation, nil
}

func relationStructType(rowType reflect.Type) (reflect.Type, error) {
	for rowType != nil && rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	if rowType == nil || rowType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("relation type must be a struct or pointer to struct, got %v", rowType)
	}
	return rowType, nil
}

func relationFieldType(fieldType reflect.Type) (reflect.Type, spec.Cardinality, error) {
	switch fieldType.Kind() {
	case reflect.Slice:
		elem := fieldType.Elem()
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		if elem.Kind() != reflect.Struct {
			return nil, "", fmt.Errorf("unsupported relation slice element kind %s", elem.Kind())
		}
		return elem, spec.CardinalityMany, nil
	case reflect.Ptr:
		if fieldType.Elem().Kind() != reflect.Struct {
			return nil, "", fmt.Errorf("unsupported relation pointer element kind %s", fieldType.Elem().Kind())
		}
		return fieldType.Elem(), spec.CardinalityOne, nil
	case reflect.Struct:
		return fieldType, spec.CardinalityOne, nil
	default:
		return nil, "", fmt.Errorf("unsupported relation field kind %s", fieldType.Kind())
	}
}
