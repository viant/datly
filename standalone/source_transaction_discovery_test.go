package standalone

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/standalone/testdata/txdiscovery"
	"github.com/viant/xdatly/handler"
	"path/filepath"
	"testing"
	"time"
)

// Metadata is prepared before generation publication. One pooled connection
// makes a late discovery borrow impossible while the parent transaction lives.
func TestLinkedMetadataPreparedBeforeTransactionPreservesFinalization(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			server, e := New(ctx, Options{Config: &config.Config{BaseDir: filepath.Join(t.TempDir(), "no-source"), Connector: "main", Connectors: []connector.Config{{Name: "main", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "fixture.db"), MaxOpenConns: 1, MaxIdleConns: 1}}, GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{txdiscovery.LinkedType.PkgPath()}}}})
			require.NoError(t, e)
			defer server.Shutdown(context.Background())
			require.NoError(t, server.Reload(ctx, 1))
			var outcome handler.Outcome
			result, e := server.InvokeComponent(ctx, exec.ComponentRequest{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: txdiscovery.LinkedType.PkgPath(), Name: "Run"}, Route: spec.RouteRef{Method: "POST", Path: "/tx-discovery"}}, Input: &txdiscovery.Input{Fail: fail}, Completion: func(o handler.Outcome) { outcome = o }})
			if fail {
				require.ErrorContains(t, e, "fixture outer rejection")
				require.Equal(t, handler.TransactionRolledBack, outcome.State())
			} else {
				require.NoError(t, e)
				require.Equal(t, 1, result.(*txdiscovery.Output).Count)
				require.True(t, outcome.CommitConfirmed())
			}
			require.Len(t, outcome.Transactions, 1)
		})
	}
}

func TestLinkedMetadataSchemaCreatedAfterReloadRequiresNewGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, e := New(ctx, Options{Config: &config.Config{BaseDir: filepath.Join(t.TempDir(), "no-source"), Connector: "main", Connectors: []connector.Config{{Name: "main", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "fixture.db"), MaxOpenConns: 1, MaxIdleConns: 1}}, GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{txdiscovery.LinkedType.PkgPath()}}}})
	require.NoError(t, e)
	defer server.Shutdown(context.Background())
	require.NoError(t, server.Reload(ctx, 1))
	call := func(name, method, path string) (any, error) {
		return server.InvokeComponent(ctx, exec.ComponentRequest{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: txdiscovery.LinkedType.PkgPath(), Name: name}, Route: spec.RouteRef{Method: method, Path: path}}, Input: &txdiscovery.ReadInput{}})
	}
	// The first generation observed a missing table; setup is a separate Datly
	// handler, and cannot silently change the generation's metadata snapshot.
	_, e = call("Setup", "POST", "/tx-discovery-setup")
	require.NoError(t, e)
	_, e = call("Late", "GET", "/tx-discovery-late")
	require.ErrorContains(t, e, "late_fixture")
	require.NoError(t, server.Reload(ctx, 2))
	value, e := call("Late", "GET", "/tx-discovery-late")
	require.NoError(t, e)
	rows := value.(*txdiscovery.LateOutput).Rows
	require.Len(t, rows, 1)
	require.Equal(t, 7, rows[0].ID)
}
