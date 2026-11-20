package associationtag

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
)

// IdentityInsert authorizes native writer code to request the scoped operation.
// External lifecycle hooks cannot name this internal type.
type IdentityInsert struct{ native *byte }

var nativeIdentity byte

// NativeIdentity is available only within Datly. A zero value is never a grant.
func NativeIdentity() IdentityInsert { return IdentityInsert{native: &nativeIdentity} }
func (i IdentityInsert) Valid() bool { return i.native == &nativeIdentity }

type IdentityDML interface {
	SupportsAssociationInsertIdentity(context.Context) (bool, error)
	OwnsAssociationValidator(IdentityInsert, xhandler.Validator) bool
	InsertAssociationIdentity(context.Context, IdentityInsert, string, any, any) error
}
