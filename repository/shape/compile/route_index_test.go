package compile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRouteIndex_AndResolve(t *testing.T) {
	tempDir := t.TempDir()
	authPath := filepath.Join(tempDir, "dql", "platform", "acl", "auth.dql")
	reportPath := filepath.Join(tempDir, "dql", "platform", "reports", "orders", "orders.dql")
	require.NoError(t, writeFile(authPath, `/* {"URI":"/v1/api/platform/acl/auth","Method":"GET"} */ SELECT 1`))
	require.NoError(t, writeFile(reportPath, `SELECT 1`))

	index, err := BuildRouteIndex([]string{authPath, reportPath})
	require.NoError(t, err)

	_, ok := index.ByRouteKey["GET:/v1/api/platform/acl/auth"]
	assert.True(t, ok)
	_, ok = index.ByRouteKey["GET:/v1/api/platform/reports/orders"]
	assert.True(t, ok) // inferred from namespace when URI is not explicitly declared

	resolved, ok := index.Resolve("../../acl/auth", reportPath)
	require.True(t, ok)
	assert.Equal(t, "GET:/v1/api/platform/acl/auth", resolved)
}

func TestRouteIndex_ResolveByAbsoluteURI(t *testing.T) {
	tempDir := t.TempDir()
	authPath := filepath.Join(tempDir, "dql", "platform", "acl", "auth.dql")
	require.NoError(t, writeFile(authPath, `/* {"URI":"/v1/api/platform/acl/auth","Method":"POST"} */ SELECT 1`))

	index, err := BuildRouteIndex([]string{authPath})
	require.NoError(t, err)

	resolved, ok := index.Resolve("POST:/v1/api/platform/acl/auth", authPath)
	require.True(t, ok)
	assert.Equal(t, "POST:/v1/api/platform/acl/auth", resolved)
}

func TestBuildRouteIndex_Conflicts(t *testing.T) {
	tempDir := t.TempDir()
	leftPath := filepath.Join(tempDir, "dql", "platform", "left", "x.dql")
	rightPath := filepath.Join(tempDir, "dql", "platform", "right", "y.dql")
	content := `/* {"URI":"/v1/api/platform/shared/resource","Method":"GET"} */ SELECT 1`
	require.NoError(t, writeFile(leftPath, content))
	require.NoError(t, writeFile(rightPath, content))

	index, err := BuildRouteIndex([]string{leftPath, rightPath})
	require.NoError(t, err)

	conflicts := index.Conflicts["GET:/v1/api/platform/shared/resource"]
	require.Len(t, conflicts, 2)
	_, ok := index.Resolve("GET:/v1/api/platform/shared/resource", leftPath)
	assert.False(t, ok)
}

func writeFile(path, content string) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}
