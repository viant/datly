package transcribe

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscoveryConditionalEmbeddedProjection(t *testing.T) {
	for _, body := range []string{
		`SELECT l.id, l.status FROM records l WHERE l.status=$Status
#if($ParentID)
AND l.parent_id=$ParentID
#end
#if($AfterID)
AND l.id>$AfterID
#end
ORDER BY l.id`,
		`SELECT l.id, l.status FROM records l
#if($IncludeArchive)
UNION ALL SELECT a.id, a.status FROM archived a
#end`,
	} {
		base := t.TempDir()
		writeSourceFile(t, base, "go.mod", "module example.com/conditional\n\ngo 1.25.0\n")
		writeSourceFile(t, base, "reader/sql/records.sql", body)
		writeSourceFile(t, base, "reader/read.dql", `#package('example.com/conditional/reader')
#setting($_ = $route('/records','GET'))
#setting($_ = $connector('main'))
#define($_ = $Status<int>(query/status).Required())
#define($_ = $ParentID<*int>(query/parentId).Optional())
#define($_ = $AfterID<*int>(query/afterId).Optional())
#define($_ = $IncludeArchive<bool>(query/includeArchive).Optional())
SELECT r.*, type(r,'Record'), CAST(r.status AS int)
FROM (${embed:sql/records.sql}) r`)
		project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/conditional/reader"}}).Compile(t.Context())
		require.NoError(t, err, body)
		require.Len(t, project.Components, 1)
		retained, err := os.ReadFile(filepath.Join(base, "reader/sql/records.sql"))
		require.NoError(t, err)
		require.Equal(t, body, string(retained))
	}
}
