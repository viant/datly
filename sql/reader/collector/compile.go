package collector

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
)

// Graph is the immutable reader-compiled relation graph for one root view.
type Graph struct {
	Root       *View
	byMetadata map[*data.View]*View
}

func (g *Graph) View(metadata *data.View) *View {
	if g == nil || metadata == nil {
		return nil
	}
	return g.byMetadata[metadata]
}

// Compile builds collector-only schema and field access without mutating data
// metadata. Row types are supplied by the reader compiler that resolved the
// component's output shape.
func Compile(root *data.View, rowTypes map[*data.View]reflect.Type) (*Graph, error) {
	compiler := graphCompiler{
		rowTypes: rowTypes,
		views:    map[*data.View]*View{},
	}
	compiledRoot, err := compiler.compileView(root)
	if err != nil {
		return nil, err
	}
	graph := &Graph{Root: compiledRoot, byMetadata: compiler.views}
	if err := graph.Validate(); err != nil {
		return nil, err
	}
	return graph, nil
}

type graphCompiler struct {
	rowTypes map[*data.View]reflect.Type
	views    map[*data.View]*View
}

func (c *graphCompiler) compileView(metadata *data.View) (*View, error) {
	if metadata == nil {
		return nil, nil
	}
	if existing := c.views[metadata]; existing != nil {
		return existing, nil
	}
	rowType := c.rowTypes[metadata]
	view := &View{View: metadata, Schema: NewSchema(rowType)}
	c.views[metadata] = view
	if metadata.Spec.SelfReference != nil {
		tree, err := NewTreePlan(view)
		if err != nil {
			return nil, err
		}
		view.Tree = tree
	}
	for _, relation := range metadata.Relations {
		compiled, err := c.compileRelation(relation, rowType)
		if err != nil {
			return nil, err
		}
		if compiled != nil {
			view.Relations = append(view.Relations, compiled)
		}
	}
	return view, nil
}

func (c *graphCompiler) compileRelation(metadata *data.Relation, parentType reflect.Type) (*Relation, error) {
	if metadata == nil {
		return nil, nil
	}
	normalizedCardinality, err := spec.NormalizeCardinality(metadata.Cardinality)
	if err != nil {
		return nil, fmt.Errorf("relation %s: %w", metadata.Name, err)
	}
	resolvedMetadata := metadata
	if normalizedCardinality != metadata.Cardinality {
		clone := *metadata
		clone.Cardinality = normalizedCardinality
		resolvedMetadata = &clone
	}
	compiled := &Relation{Relation: resolvedMetadata}
	if metadata.Of != nil {
		child, err := c.compileView(metadata.Of.View)
		if err != nil {
			return nil, err
		}
		compiled.Of = &RelationRef{
			RelationRef: metadata.Of,
			View:        child,
			On:          compileLinks(metadata.Of.On, c.rowTypes[metadata.Of.View]),
		}
	}
	compiled.On = compileLinks(metadata.On, parentType)
	if !metadata.IsOutput() && parentType != nil && strings.TrimSpace(metadata.Holder) != "" {
		compiled.HolderField = xunsafe.FieldByName(parentType, metadata.Holder)
		if compiled.HolderField == nil {
			return nil, fmt.Errorf("relation %s holder %q was not found on %s", metadata.Name, metadata.Holder, parentType)
		}
		if compiled.HolderField.Type.Kind() == reflect.Slice {
			compiled.HolderSlice = xunsafe.NewSlice(compiled.HolderField.Type)
		}
	}
	return compiled, nil
}

func compileLinks(metadata data.Links, rowType reflect.Type) Links {
	result := make(Links, 0, len(metadata))
	for _, link := range metadata {
		if link == nil {
			continue
		}
		result = append(result, &Link{Link: link, XField: compileScannedField(rowType, link.Field)})
	}
	return result
}

func compileScannedField(rowType reflect.Type, name string) *xunsafe.Field {
	for rowType != nil && rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	if rowType == nil || rowType.Kind() != reflect.Struct {
		return nil
	}
	field, ok := rowType.FieldByName(name)
	if !ok {
		return nil
	}
	if tag := sqlxio.ParseTag(field.Tag); tag != nil && tag.Transient {
		return nil
	}
	return xunsafe.FieldByName(rowType, name)
}
