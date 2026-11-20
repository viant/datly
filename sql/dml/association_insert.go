package dml

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/associationtag"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"strings"

	"github.com/viant/sqlx/metadata/info/dialect"
)

// SupportsAssociationInsertIdentity identifies the native allocation strategy
// for which an association read and the transient insert may self-lock.
func (d *Data) SupportsAssociationInsertIdentity(ctx context.Context) (bool, error) {
	product, err := d.Dialect(ctx)
	if err != nil {
		return false, err
	}
	return product != nil && product.SequenceStrategy(d.owner().sequenceStrategy) == dialect.PresetIDWithTransientTransaction && strings.EqualFold(product.Product.Name, "MySQL"), nil
}

// InsertAssociationWithIdentity journals a singleton mapped insert. key is the
// address of the generated field on row. The SQLX backfill is verified against
// the actual insert before any later journal operation can execute.
func (d *Data) InsertAssociationWithIdentity(ctx context.Context, table string, row, key any) error {
	supported, err := d.SupportsAssociationInsertIdentity(ctx)
	if err != nil {
		return err
	}
	if !supported {
		return fmt.Errorf("unsupported association identity source")
	}
	if err := validateLastInsertID(key); err != nil {
		return err
	}
	value := reflect.ValueOf(row)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct || reflect.ValueOf(key).Elem().Int() != 0 {
		return fmt.Errorf("association identity requires a singleton row with an unset generated key")
	}
	return d.append(dataOperation{kind: dataOpInsert, table: table, data: row, generatedKey: key, appendBarrier: true})
}

// OwnsAssociationValidator binds the delayed-identity eligibility decision to
// the same native validator and component frame used during both writer passes.
func (d *Data) OwnsAssociationValidator(permit associationtag.IdentityInsert, v xhandler.Validator) bool {
	native, ok := v.(dataValidation)
	if !permit.Valid() || !ok || native.data != d || native.service == nil {
		return false
	}
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.frameworkValidator == native.service
}
