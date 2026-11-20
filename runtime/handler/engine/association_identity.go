package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/internal/associationtag"
	xhandler "github.com/viant/xdatly/handler"
)

// Association identity grants come from registered component settings. They
// apply only to the mapped native insertion and never widen the public Flusher.
func splitAssociationTables(handler any, tables []string) (ordinary, identity []string) {
	mapping, ok := handler.(interface {
		AssociationIdentityPrefix(associationtag.IdentityInsert, string) bool
	})
	if !ok {
		return tables, nil
	}
	for _, table := range tables {
		if mapping.AssociationIdentityPrefix(associationtag.NativeIdentity(), table) {
			identity = append(identity, table)
		} else {
			ordinary = append(ordinary, table)
		}
	}
	return
}

type associationIdentityData interface {
	OwnsAssociationValidator(associationtag.IdentityInsert, xhandler.Validator) bool
	SupportsAssociationInsertIdentity(context.Context) (bool, error)
	InsertAssociationWithIdentity(context.Context, string, any, any) error
}

func (c dmlCapability) SupportsAssociationInsertIdentity(ctx context.Context) (bool, error) {
	service, ok := c.service.(associationIdentityData)
	if !ok {
		return false, nil
	}
	return service.SupportsAssociationInsertIdentity(ctx)
}

func (c dmlCapability) InsertAssociationIdentity(ctx context.Context, permit associationtag.IdentityInsert, table string, row, key any) error {
	if !permit.Valid() {
		return fmt.Errorf("native association insert authority required")
	}
	if err := c.guard.check("InsertAssociationIdentity"); err != nil {
		return err
	}
	if c.associationAuthority == nil || !c.associationAuthority.tables[strings.ToLower(table)] {
		return fmt.Errorf("association insert has no prefix authority for %s", table)
	}
	service, ok := c.service.(associationIdentityData)
	if !ok {
		return fmt.Errorf("native association identity unavailable")
	}
	flusher, ok := c.service.(xhandler.Flusher)
	if !ok {
		return fmt.Errorf("association flusher unavailable")
	}
	if err := service.InsertAssociationWithIdentity(ctx, table, row, key); err != nil {
		return err
	}
	return (flusherCapability{guard: c.guard, service: flusher, authority: c.associationAuthority}).Flush(ctx, table)
}

func (c dmlCapability) OwnsAssociationValidator(permit associationtag.IdentityInsert, v xhandler.Validator) bool {
	native, ok := c.service.(associationIdentityData)
	return ok && permit.Valid() && native.OwnsAssociationValidator(permit, v)
}
