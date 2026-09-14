package application_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	jobstorage "github.com/viant/datly/gateway/async"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/jobs"
	dsql "github.com/viant/datly/sql"
)

func TestJobSchemaFailurePreventsWatcherStartupSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	store, err := (bootstrap.JobStoreConfig{SQL: &dsql.SQLComponent{DB: h.DB}, DisableTableCreation: true}).NewStore(ctx)
	require.Error(t, err)
	require.Nil(t, store)
	root := t.TempDir()
	event := filepath.Join(root, "pending.job")
	require.NoError(t, os.WriteFile(event, []byte(`{}`), 0600))
	manager, err := application.New(nil, application.WithAsync(application.AsyncConfig{Store: store, Authorize: func(context.Context, jobs.Access) error { return nil }, Watch: jobstorage.WatchConfig{JobURL: root}}))
	require.Error(t, err)
	require.Nil(t, manager)
	require.FileExists(t, event)
}
