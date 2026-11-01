package reader

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/collector"
	xhandler "github.com/viant/xdatly/handler"
)

// projectionGraph reuses the prepared source's binding and native relation
// plans. Only its parent shape/key accessors are projection-specific.
type projectionGraph struct {
	bindingContext context.Context
	rowType        reflect.Type
	service        *Service
	session        *Session
	input          reflect.Value
	binder         xhandler.Binder
	selectors      invocationSelectors
	graph          *collector.Graph
}

func (g *projectionGraph) RowType() reflect.Type { return g.rowType }

func (r *projectionReader) PrepareProjectionGraph(ctx context.Context, request dexec.ProjectionGraphRequest) (dexec.ProjectionGraph, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.session == nil || r.service == nil {
		return nil, fmt.Errorf("projection source graph is unavailable")
	}
	if request.RowType == nil || request.RowType.Kind() != reflect.Struct || len(request.Relations) == 0 {
		return nil, fmt.Errorf("projection graph requires a row type and declared relations")
	}
	source := r.session.Artifact.Root
	root := *source.View
	root.Relations = nil
	root.Spec.SelfReference = nil
	root.Columns = nil
	root.SelectorFields = nil
	fields := make([]reflect.StructField, request.RowType.NumField())
	for i := range fields {
		fields[i] = request.RowType.Field(i)
	}
	rowTypes := map[*data.View]reflect.Type{}
	selected := map[string]bool{}
	for _, requested := range request.Relations {
		var relation *data.Relation
		for _, candidate := range source.View.Relations {
			if candidate != nil && (candidate.Holder == requested.Name || candidate.Name == requested.Name) {
				if relation != nil && relation != candidate {
					return nil, fmt.Errorf("ambiguous projection relation %s", requested.Name)
				}
				relation = candidate
			}
		}
		if relation == nil || relation.Of == nil || relation.Of.View == nil || relation.IsOutput() {
			return nil, fmt.Errorf("unknown projection relation %s", requested.Name)
		}
		if selected[relation.Holder] {
			return nil, fmt.Errorf("duplicate projection relation %s", requested.Name)
		}
		selected[relation.Holder] = true
		cardinality, err := spec.NormalizeCardinality(relation.Cardinality)
		if err != nil || cardinality != spec.CardinalityOne {
			return nil, fmt.Errorf("projection relation %s must be to-one", requested.Name)
		}
		child := r.session.Artifact.ViewPlanFor(relation.Of.View)
		if child == nil || child.Collector == nil || child.Partitioner != nil || len(child.View.Relations) > 0 || child.View.Spec.SelfReference != nil {
			return nil, fmt.Errorf("projection relation %s requires a nonpartitioned leaf view", requested.Name)
		}
		childType := child.Collector.Schema.RowType()
		if childType.Kind() != reflect.Pointer {
			childType = reflect.PointerTo(childType)
		}
		if childType.Implements(reflect.TypeFor[xhandler.OnFetcher]()) || childType.Implements(reflect.TypeFor[xhandler.OnRelationer]()) {
			return nil, fmt.Errorf("projection relation %s has lifecycle hooks; projection labels require a hook-free child", requested.Name)
		}
		if len(relation.On) == 0 || len(relation.On) != len(relation.Of.On) || len(requested.Keys) != len(relation.On) {
			return nil, fmt.Errorf("projection relation %s requires every key", requested.Name)
		}
		originalType := source.Collector.Schema.RowType()
		for originalType.Kind() == reflect.Pointer {
			originalType = originalType.Elem()
		}
		holder, ok := originalType.FieldByName(relation.Holder)
		if !ok || holder.PkgPath != "" || holder.Type.Kind() != reflect.Pointer || holder.Type.Elem().Kind() != reflect.Struct || holder.Tag.Get("internal") == "true" || strings.Split(holder.Tag.Get("json"), ",")[0] == "-" {
			return nil, fmt.Errorf("projection relation %s requires a public pointer holder", requested.Name)
		}
		for _, link := range relation.Of.On {
			if link == nil {
				return nil, fmt.Errorf("projection relation %s has an invalid child key", requested.Name)
			}
			if field, ok := childType.Elem().FieldByName(link.Field); ok && field.Tag.Get("relationKey") != "" {
				return nil, fmt.Errorf("projection relation %s requires SQL-backed child keys", requested.Name)
			}
		}
		if requested.Output == "" || strings.ContainsAny(requested.Output, "\"\\`\n\r") {
			return nil, fmt.Errorf("invalid projection relation output")
		}
		for _, f := range fields {
			if f.Name == holder.Name || strings.Split(f.Tag.Get("json"), ",")[0] == requested.Output {
				return nil, fmt.Errorf("projection relation %s conflicts with a projected field", requested.Name)
			}
		}
		// Retain the original typed child and its native decoder. Hook-bearing
		// children are rejected instead of bypassing their output/security contract.
		fields = append(fields, reflect.StructField{Name: holder.Name, Type: holder.Type, Tag: reflect.StructTag(fmt.Sprintf("json:%q sqlx:\"-\"", requested.Output))})
		cloned := *relation
		cloned.On = nil
		used := map[string]bool{}
		for _, link := range relation.On {
			if link == nil {
				return nil, fmt.Errorf("projection relation %s has an invalid key", requested.Name)
			}
			output := ""
			for _, key := range requested.Keys {
				if key.Source == link.OutputColumn() {
					if output != "" {
						return nil, fmt.Errorf("ambiguous projection key %s", key.Source)
					}
					output = key.Output
				}
			}
			if output == "" || used[output] {
				return nil, fmt.Errorf("projection relation %s is missing an unambiguous key %s", requested.Name, link.OutputColumn())
			}
			used[output] = true
			var field *reflect.StructField
			for i := 0; i < request.RowType.NumField(); i++ {
				f := request.RowType.Field(i)
				if strings.Split(f.Tag.Get("sqlx"), ",")[0] == output {
					if field != nil {
						return nil, fmt.Errorf("ambiguous projection output %s", output)
					}
					field = &f
				}
			}
			if field == nil {
				return nil, fmt.Errorf("projection key %s is not in the result", output)
			}
			original, ok := originalType.FieldByName(link.Field)
			if !ok || original.Tag.Get("relationKey") != "" || indirectProjectionType(original.Type) != indirectProjectionType(field.Type) {
				return nil, fmt.Errorf("projection key %s does not match its source type", output)
			}
			mapped := *link
			mapped.Field = field.Name
			mapped.Column = output
			mapped.Output = output
			mapped.Namespace = ""
			cloned.On = append(cloned.On, &mapped)
		}
		ref := *relation.Of
		ref.MatchStrategy = data.MatchSequential
		cloned.Of = &ref
		root.Relations = append(root.Relations, &cloned)
		rowTypes[child.View] = child.Collector.Schema.RowType()
	}
	rowType := reflect.StructOf(fields)
	rowTypes[&root] = reflect.PointerTo(rowType)
	graph, err := collector.Compile(&root, rowTypes)
	if err != nil {
		return nil, err
	}
	// A fresh session avoids copying mutexes/observations and never runs source
	// initialization, OnFetch, OnRelation, or output finalization.
	session := r.service.projectionRelationSession(r.session)
	return &projectionGraph{bindingContext: r.bindingContext, rowType: rowType, service: r.service, session: session, input: r.input, binder: r.binder, selectors: r.selectors, graph: graph}, nil
}

func indirectProjectionType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func (s *Service) projectionRelationSession(source *Session) *Session {
	return &Session{Component: source.Component, InputType: source.InputType, OutputType: source.OutputType, Artifact: source.Artifact, SQL: source.SQL, ReadCaches: source.ReadCaches, Parameters: source.Parameters, RefreshCache: source.RefreshCache, CacheOnly: source.CacheOnly, QueryScope: source.QueryScope, ForUpdate: append([]string(nil), source.ForUpdate...), recorder: source.recorder, metricScope: source.metricScope, observations: source.observations, pendingScope: source.pendingScope}
}

func (g *projectionGraph) Load(ctx context.Context, rows any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx = projectionBindingContext{Context: ctx, source: g.bindingContext}
	value := reflect.ValueOf(rows)
	expected := reflect.SliceOf(reflect.PointerTo(g.rowType))
	if !value.IsValid() || value.Type() != expected {
		return nil, fmt.Errorf("projection graph requires %s", expected)
	}
	if value.Len() == 0 {
		return rows, nil
	}
	dest := reflect.New(expected)
	dest.Elem().Set(reflect.MakeSlice(expected, 0, value.Len()))
	root := collector.NewCollector(g.graph.Root, dest.Interface(), false)
	if err := root.AppendSlice(ctx, rows); err != nil {
		return nil, err
	}
	root.Fetched()
	if err := g.service.bindRelations(ctx, g.session, g.input, g.binder, g.selectors, root); err != nil {
		return nil, err
	}
	if err := root.MergeData(); err != nil {
		return nil, err
	}
	if err := root.ValidateToOne(); err != nil {
		return nil, err
	}
	return dest.Elem().Interface(), nil
}

// Source binding values (including frame/auth context) remain authoritative;
// cancellation and deadlines belong to the active projection caller.
type projectionBindingContext struct {
	context.Context
	source context.Context
}

func (c projectionBindingContext) Value(key any) any {
	if c.source != nil {
		return c.source.Value(key)
	}
	// Absence is part of the prepared source's authority. The active caller
	// supplies cancellation/deadlines only, never additional binding values.
	return nil
}
