package operator

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/service/executor/uow"
)

func TestFinishOperation(t *testing.T) {
	t.Run("preserves structured output for operation error", func(t *testing.T) {
		ctx, scope, _ := uow.NewRoot(context.Background(), "test")
		expectedOutput := &struct{}{}
		expectedErr := errors.New("handler error")

		actualOutput, actualErr := finishOperation(ctx, scope, expectedOutput, expectedErr)

		require.ErrorIs(t, actualErr, expectedErr)
		assert.Same(t, expectedOutput, actualOutput)
	})

	t.Run("discards output when finish introduces an error", func(t *testing.T) {
		ctx, scope, frame := uow.NewRoot(context.Background(), "test")
		expectedOutput := &struct{}{}
		expectedErr := errors.New("generation error")
		buffer := frame.NewBuffer(
			func(context.Context) (*sql.DB, error) {
				return nil, expectedErr
			},
			nil,
			func(context.Context, *sql.Tx, any) error {
				return nil
			},
		)
		require.NoError(t, buffer.Append(struct{}{}))

		actualOutput, actualErr := finishOperation(ctx, scope, expectedOutput, nil)

		require.ErrorIs(t, actualErr, expectedErr)
		assert.Nil(t, actualOutput)
	})

	t.Run("preserves output when finish succeeds", func(t *testing.T) {
		ctx, scope, _ := uow.NewRoot(context.Background(), "test")
		expectedOutput := &struct{}{}

		actualOutput, actualErr := finishOperation(ctx, scope, expectedOutput, nil)

		require.NoError(t, actualErr)
		assert.Same(t, expectedOutput, actualOutput)
	})
}
