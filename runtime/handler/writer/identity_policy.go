package writer

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/dialectcontext"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

const assignedUpdateIdentity = "assigned-update"

func requestedNonzeroIdentity(record *Record, entity reflect.Value) bool {
	if record == nil || len(record.Keys) != 1 {
		return false
	}
	key := record.Keys[0]
	if !supplied(entity, key) {
		return false
	}
	value := entity.FieldByIndex(key.Index)
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	return value.IsValid() && !value.IsZero()
}
func validateWriterIdentityPolicy(record *Record, operation string) error {
	if record == nil {
		return nil
	}
	if record.WriterIdentityPolicy != "" {
		if record.WriterIdentityPolicy != assignedUpdateIdentity {
			return fmt.Errorf("writerIdentity must be assigned-update")
		}
		if operation != "patch" || record.Auxiliary {
			return fmt.Errorf("writerIdentity assigned-update requires a PATCH writable role")
		}
		if len(record.Keys) != 1 || !numericField(record.EntityType, record.Keys[0]) {
			return fmt.Errorf("writerIdentity assigned-update requires one numeric identity")
		}
		if len(record.Relations) != 0 {
			return fmt.Errorf("writerIdentity assigned-update requires a leaf role")
		}
	}
	for _, relation := range record.Relations {
		if err := validateWriterIdentityPolicy(relation.Child, operation); err != nil {
			return err
		}
	}
	return nil
}
func (p *Program) checkMissingIdentityGuards(ctx context.Context, binder xhandler.Binder, record *Record, entity reflect.Value) error {
	if record.ConcurrencyToken != nil && supplied(entity, *record.ConcurrencyToken) {
		return &xhandler.Conflict{Entity: record.Path, Field: record.ConcurrencyToken.Name, Reason: "unmatched identity cannot satisfy a concurrency token"}
	}
	if record.MutationPredicateGroup != nil {
		predicateCtx, err := mutationPredicateContext(ctx, binder)
		if err != nil {
			return err
		}
		if dialectcontext.Dialect(predicateCtx) == nil {
			return fmt.Errorf("mutation predicate %s requires a dialect", record.Path)
		}
		criteria, err := p.metadata.Predicates.Criteria(predicateCtx, binder, *record.MutationPredicateGroup)
		if err != nil {
			return err
		}
		if criteria != nil {
			return &xhandler.Conflict{Entity: record.Path, Reason: "unmatched identity cannot satisfy active mutation criteria"}
		}
	}
	return nil
}
