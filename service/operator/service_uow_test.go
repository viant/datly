package operator

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/datly/service/executor/uow"
)

func TestSealCreatedFrameDoesNotSealBorrowedRoot(t *testing.T) {
	ctx, _, root := uow.NewRoot(context.Background(), "root")
	sealCreatedFrame(false, root)
	if _, _, _, _, err := uow.Enter(ctx, "nested"); err != nil {
		t.Fatalf("borrowed root was sealed: %v", err)
	}
}

func TestSealCreatedFrameSealsChild(t *testing.T) {
	ctx, _, _ := uow.NewRoot(context.Background(), "root")
	childCtx := uow.PrepareChild(ctx, uow.RelationImperative, "")
	childCtx, _, child, _, err := uow.Enter(childCtx, "child")
	if err != nil {
		t.Fatal(err)
	}
	sealCreatedFrame(true, child)
	if _, _, _, _, err = uow.Enter(childCtx, "reuse"); !errors.Is(err, uow.ErrFrameSealed) {
		t.Fatalf("expected sealed child, got %v", err)
	}
}
