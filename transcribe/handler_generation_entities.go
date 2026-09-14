package transcribe

import (
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	handlerplan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

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
