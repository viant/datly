package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_SyncChanges_RefreshDisabled(t *testing.T) {
	service := &Service{
		refreshDisabled: true,
	}

	changed, err := service.SyncChanges(context.Background())
	require.NoError(t, err)
	assert.False(t, changed)
}
