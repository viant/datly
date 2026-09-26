package transcribe

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/sqlx/testutil/sqlfault"
)

func TestCompilerDiscoveryMetadataScope(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, name TEXT)"))
	var product, session, output atomic.Int32
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase != "query" {
			return nil
		}
		switch {
		case strings.Contains(call.SQL, "sqlite_version()"):
			product.Add(1)
		case strings.Contains(call.SQL, "pragma_database_list"):
			session.Add(1)
		case !strings.Contains(call.SQL, "pragma_"):
			output.Add(1)
		}
		return nil
	})
	refiner := column.New(column.Connections{"main": db})
	compile := func() error {
		compiled, err := NewCompiler().Compile(ctx, &Source{
			Name: "Records", Scope: "example.com/records", Connector: "main", ColumnRefiner: refiner,
			Text: `#setting($_ = $route('/records', 'GET'))
#define($_ = $Other<?>(view/Other).Optional() /* SELECT r.id, r.name FROM records r */)
SELECT r.id, r.name FROM records r`,
		})
		if err != nil {
			return err
		}
		if len(compiled.Component.RootView.Columns) != 2 || len(compiled.Component.Views) != 1 || len(compiled.Component.Views[0].Columns) != 2 {
			return fmt.Errorf("root and independent view must both receive discovered columns")
		}
		return nil
	}
	for i := int32(1); i <= 2; i++ {
		require.NoError(t, compile())
		require.Equal(t, i, product.Load(), "metadata must be shared across phases, not across compilations")
		require.Equal(t, i, session.Load())
		require.Equal(t, 2*i, output.Load())
	}
	const workers = 4
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() { errs <- compile() }()
	}
	for i := 0; i < workers; i++ {
		require.NoError(t, <-errs)
	}
	require.EqualValues(t, 2+workers, product.Load())
	require.EqualValues(t, 2+workers, session.Load())
	require.EqualValues(t, 2*(2+workers), output.Load())
}
