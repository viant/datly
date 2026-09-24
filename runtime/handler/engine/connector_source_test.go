package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	dexec "github.com/viant/datly/exec"
)

type connectorOnly struct{}

func (connectorOnly) Connector(context.Context, string) (*sql.DB, error) {
	panic("engine must not construct data sources from raw connectors")
}

type connectorSourceStub struct {
	connectorOnly
	source dexec.DataSource
	err    error
}

func (s connectorSourceStub) ConnectorDataSource(context.Context, string) (dexec.DataSource, error) {
	return s.source, s.err
}

func TestTransactionSQLRequiresInjectedSourceProvider(t *testing.T) {
	provider := transactionSQLProvider{scope: &dataScope{connectors: connectorOnly{}}}
	if _, err := provider.Connector(context.Background(), "main"); err == nil || !strings.Contains(err.Error(), "data-source provider") {
		t.Fatalf("missing source provider: %v", err)
	}
	for _, source := range []dexec.DataSource{nil, &staticDataSource{}} {
		provider.scope.connectors = connectorSourceStub{source: source}
		if _, err := provider.Connector(context.Background(), "main"); !errors.Is(err, ErrUnknownDatabaseIdentity) {
			t.Fatalf("unidentified source accepted: %v", err)
		}
	}
	want := errors.New("connector denied")
	provider.scope.connectors = connectorSourceStub{err: want}
	if _, err := provider.Connector(context.Background(), "main"); !errors.Is(err, want) {
		t.Fatalf("source resolution error lost: %v", err)
	}
}
