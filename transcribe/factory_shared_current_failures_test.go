package transcribe

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

func TestFactorySharedCurrentRejectsCompetingAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, projection, current, destination, extraColumn, want string
		foreign, inferred                                         bool
	}{
		{name: "foreign typed Current with matching basename", foreign: true, want: "does not match package type"},
		{name: "incompatible inferred type from different table", inferred: true, current: "SELECT * FROM other_records", want: "conflicts with body field"},
		{name: "explicit any cannot bypass concrete body", current: "SELECT r.*, CAST(r.TARGET AS any) FROM records r", want: "CAST column"},
		{name: "explicit incompatible type", current: "SELECT r.*, CAST(r.TARGET AS int) FROM records r", want: "CAST column"},
		{name: "current-only column cannot widen body", extraColumn: ", EXTRA TEXT", want: "no SQLX field"},
		{name: "conflicting file", destination: "different.go", want: "destination"},
		{name: "competing body owner", projection: "SELECT r.*, n.*, type(r,'archive.Res'), type(n,'archive.Res'), dest(r,'res.go') FROM records r JOIN records n ON n.ID=r.ID", want: "compete for type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			projection := "SELECT r.ID, r.TARGET, r.EXCLUSION, type(r,'archive.Res'), dest(r,'res.go'), required(r.TARGET), required(r.EXCLUSION) FROM records r"
			if tc.projection != "" {
				projection = tc.projection
			}
			current := tc.current
			if current == "" {
				current = "SELECT * FROM records"
			}
			destination := tc.destination
			if destination == "" {
				destination = "res.go"
			}
			source.Text = strings.Replace(source.Text, factoryInputProjection, projection, 1)
			source.Text = strings.Replace(source.Text, "#define($_ = $Status", "#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('archive.Res').Dest('"+destination+"') /* "+current+" */)\n#define($_ = $Status", 1)
			if tc.foreign {
				writeSourceHandlerFile(t, root, "foreign/types.go", `package foreign
import "reflect"
type Res struct { ID *int; TARGET string; EXCLUSION string }
var _anchorRes = reflect.TypeFor[Res]()
`)
				source.Text = strings.Replace(source.Text, "#import('archive'", "#import('foreign','"+handlerFixtureModule+"/foreign')\n#import('archive'", 1)
				source.Text = strings.Replace(source.Text, "$Current<?>", "$Current<[]*foreign.Res>", 1)
			}
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE records(ID INTEGER PRIMARY KEY, TARGET TEXT NOT NULL, EXCLUSION TEXT NOT NULL" + tc.extraColumn + ")")
			require.NoError(t, err)
			if tc.inferred {
				_, err = db.Exec("CREATE TABLE other_records(ID INTEGER PRIMARY KEY, TARGET INTEGER NOT NULL, EXCLUSION TEXT NOT NULL)")
				require.NoError(t, err)
			}
			source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
			before := sourceHandlerSnapshot(t, root)
			_, err = (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, before, sourceHandlerSnapshot(t, root), "failed authoring must not publish or widen the generated body")
		})
	}
}
