package transcribe

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	handlerplan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	sqlio "github.com/viant/sqlx/io"
	xshape "github.com/viant/x/shape"
	"go/ast"
	"go/token"
	"reflect"
	"strings"
)

// applyEntitySetters retains only public generated entity accessors. Mutation
// state, snapshots, invariants, and phase programs belong to the universal
// runtime writer and are never emitted into an application package.
func (g *handlerGeneration) applyEntitySetters(asset *handlergo.EntityAsset) error {
	if asset == nil || asset.File == nil {
		g.input.EntitySupport = nil
		return nil
	}
	wanted := map[string]handlergo.EntityMethod{}
	for _, method := range asset.Methods {
		if strings.HasPrefix(method.Name, "Set") || strings.HasPrefix(method.Name, "Get") || strings.HasPrefix(method.Name, "Project") {
			wanted[method.Receiver+"."+method.Name] = method
		}
	}
	if len(wanted) == 0 {
		g.input.EntitySupport = nil
		return nil
	}
	selected := &ast.File{Name: ast.NewIdent(asset.File.Name.Name)}
	usedAliases := map[string]bool{}
	for _, declaration := range asset.File.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok || method.Recv == nil || len(method.Recv.List) != 1 {
			continue
		}
		receiver := method.Recv.List[0].Type
		if pointer, ok := receiver.(*ast.StarExpr); ok {
			receiver = pointer.X
		}
		name, ok := receiver.(*ast.Ident)
		if !ok {
			continue
		}
		if _, ok = wanted[name.Name+"."+method.Name.Name]; !ok {
			continue
		}
		selected.Decls = append(selected.Decls, method)
		ast.Inspect(method, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok {
				if qualifier, ok := selector.X.(*ast.Ident); ok {
					usedAliases[qualifier.Name] = true
				}
			}
			return true
		})
	}
	for _, declaration := range asset.File.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.IMPORT {
			continue
		}
		imports := &ast.GenDecl{Tok: token.IMPORT, Lparen: group.Lparen}
		for _, item := range group.Specs {
			specification := item.(*ast.ImportSpec)
			alias := ""
			if specification.Name != nil {
				alias = specification.Name.Name
			}
			if alias != "" && usedAliases[alias] {
				imports.Specs = append(imports.Specs, specification)
			}
		}
		if len(imports.Specs) > 0 {
			selected.Decls = append([]ast.Decl{imports}, selected.Decls...)
		}
	}
	filtered := &handlergo.EntityAsset{File: selected}
	for _, method := range asset.Methods {
		if _, ok := wanted[method.Receiver+"."+method.Name]; ok {
			filtered.Methods = append(filtered.Methods, method)
		}
	}
	result := &gen.EntitySupportAsset{File: filtered.File}
	for _, method := range filtered.Methods {
		result.Methods = append(result.Methods, gen.EntityMethod{Receiver: method.Receiver, Name: method.Name, ValueType: method.ValueType, Getter: method.Getter, Signature: method.Signature})
	}
	g.input.EntitySupport = result
	return nil
}

func (g *handlerGeneration) applyEntitySupport(asset *handlergo.EntityAsset) {
	if asset == nil {
		g.input.EntitySupport = nil
		return
	}
	result := &gen.EntitySupportAsset{File: asset.File, CaptureFunction: asset.CaptureFunction, SnapshotType: asset.SnapshotType, SyncContextType: asset.SyncContextType, SyncMethod: asset.SyncMethod}
	for _, association := range asset.Associations {
		result.Associations = append(result.Associations, gen.EntityAssociation{Identity: association.Identity, Path: append([]string(nil), association.Path...), Field: association.Field, StateType: association.StateType, KeyType: association.KeyType, KeyAdapterType: association.KeyAdapterType, CurrentKeyFunction: association.CurrentKeyFunction})
	}
	for _, method := range asset.Methods {
		result.Methods = append(result.Methods, gen.EntityMethod{Receiver: method.Receiver, Name: method.Name, ValueType: method.ValueType, Getter: method.Getter, Signature: method.Signature})
	}
	for _, invariant := range asset.Invariants {
		result.Invariants = append(result.Invariants, gen.EntityInvariant{Identity: invariant.Identity, Path: append([]string(nil), invariant.Path...), Group: invariant.Group, BackfillFunction: invariant.BackfillFunction})
	}
	g.input.EntitySupport = result
}

func (g *handlerGeneration) refineLinkedEntity(record *handlerplan.RecordPlan, base string, imports []spec.ImportSpec) error {
	if record.Auxiliary {
		return nil
	}
	entity := &handlerplan.EntityPlan{}
	record.Entity = entity
	if g.input.TypeResolver == nil {
		return nil
	}
	descriptor, err := g.input.TypeResolver.Descriptor(base)
	if err != nil {
		return err
	}
	if descriptor == nil {
		return nil
	}
	shape := xshape.New(descriptor, g.input.TypeResolver.Descriptor)
	fields, err := shape.Fields()
	if err != nil {
		return err
	}
	if err = g.refineLinkedSelfRelations(record, fields); err != nil {
		return err
	}
	for _, key := range record.Keys {
		for _, field := range fields {
			if field.Exported && strings.EqualFold(field.Name, key.Field) {
				key.Field = field.Name
				expression := field.TypeExpr
				if field.ReflectedType != nil {
					expression, err = (xshape.Resolver{Qualifier: func(location, suggested string) string {
						for _, item := range imports {
							if item.Package == location {
								return item.Alias
							}
						}
						return suggested
					}}).Expression(field.ReflectedType)
					if err != nil {
						return err
					}
				}
				key.Type = spec.TypeRef{Name: expression}
				break
			}
		}
		entity.Keys = append(entity.Keys, key)
	}
	for _, field := range fields {
		if !field.Exported || field.Tag.Get("setMarker") != "true" {
			continue
		}
		entity.MarkerField = field.Name
		if field.ReflectedType != nil {
			entity.MarkerPointer = field.ReflectedType.Kind() == reflect.Pointer
		} else {
			reference, err := (xshape.Resolver{}).Reference(field.TypeExpr)
			if err != nil {
				return err
			}
			entity.MarkerPointer = len(reference.Wrappers) > 0 && reference.Wrappers[0].Kind == xshape.WrapperPointer
		}
		markers, err := shape.FieldsAt(field.Name)
		if err != nil {
			return err
		}
		for _, marker := range markers {
			if !marker.Exported {
				continue
			}
			kind, ok := marker.BuiltinType()
			if marker.ReflectedType != nil && marker.ReflectedType.Kind() == reflect.Bool {
				kind, ok = "bool", true
			}
			if !ok || kind != "bool" {
				continue
			}
			for _, business := range fields {
				if business.Exported && business.Name == marker.Name {
					expression := business.TypeExpr
					if business.ReflectedType != nil {
						expression, err = (xshape.Resolver{Qualifier: func(location, suggested string) string {
							for _, item := range imports {
								if item.Package == location {
									return item.Alias
								}
							}
							return suggested
						}}).Expression(business.ReflectedType)
						if err != nil {
							return err
						}
					}
					planned := handlerplan.EntityField{Name: business.Name, Path: handlerplan.FieldPath{business.Name}, Type: spec.TypeRef{Name: expression}, Writable: true}
					if record.Write.DeleteMarker.Field == business.Name && !sqlio.ParseTag(reflect.StructTag(business.Tag)).Transient {
						return fmt.Errorf("delete_marker linked field %s must declare sqlx:\"-\"", business.Name)
					}
					for _, key := range entity.Keys {
						if key.Field == business.Name {
							planned.Identity = true
						}
					}
					if _, found := business.Tag.Lookup(tag.ViewName); found {
						planned.Relation = true
						planned.Writable = false
					}
					if value, found := business.Tag.Lookup(tag.SelfName); found {
						if _, err = tag.ParseSelf(value); err != nil {
							return err
						}
						planned.Self = true
						planned.Relation = true
						planned.Writable = true
						canonical, err := business.CanonicalType()
						if err != nil {
							return err
						}
						planned.Type = spec.TypeRef{Name: canonical}
					}
					for _, relation := range record.Relations {
						if relation != nil && len(relation.FieldPath) > 0 && relation.FieldPath[len(relation.FieldPath)-1] == business.Name {
							planned.Relation = true
							planned.Path = append(handlerplan.FieldPath(nil), relation.FieldPath...)
							planned.Writable = relation.Child != nil && !relation.Child.Auxiliary
						}
					}
					entity.Fields = append(entity.Fields, planned)
					break
				}
			}
		}
		break
	}
	return nil
}
