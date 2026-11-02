package generated

import (
	"context"
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

// Authored fixture hooks advance tokens through the public lifecycle contract.
type DocumentHooks struct{}

func DocumentHooksDatlyType() reflect.Type { return reflect.TypeFor[DocumentHooks]() }

var DocumentHooksDatly = DocumentHooksDatlyType()
var DocumentHooksHooks = new(DocumentHooks)

func (*DocumentHooks) Init(ctx context.Context, row *Document, state xhandler.LifecycleContext[Document, Ledger, GuardOutput]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.Previous == nil || state.Previous.Revision == nil {
		return fmt.Errorf("previous document required")
	}
	next := *state.Previous.Revision + 1
	row.SetRevision(&next)
	return nil
}
func (*DocumentHooks) Validate(ctx context.Context, row *Document, state xhandler.LifecycleContext[Document, Ledger, GuardOutput]) error {
	return ctx.Err()
}

type DraftHooks struct{}

func DraftHooksDatlyType() reflect.Type { return reflect.TypeFor[DraftHooks]() }

var DraftHooksDatly = DraftHooksDatlyType()
var DraftHooksHooks = new(DraftHooks)

func (*DraftHooks) Init(ctx context.Context, row *Draft, state xhandler.LifecycleContext[Draft, Document, GuardOutput]) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.Previous == nil || state.Previous.Revision == nil {
		return fmt.Errorf("previous draft required")
	}
	if row.ShouldDelete {
		return nil
	}
	next := *state.Previous.Revision + 1
	row.SetRevision(&next)
	return nil
}
func (*DraftHooks) Validate(ctx context.Context, row *Draft, state xhandler.LifecycleContext[Draft, Document, GuardOutput]) error {
	return ctx.Err()
}
