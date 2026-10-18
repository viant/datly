package transcribe

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceHandlerSelectorTransportGenerationReloadRepeat(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	const business = `package business
import("context";"github.com/viant/xdatly/handler")
type Input struct {
 Fields []string ` + "`parameter:\"Fields,kind=query,in=fields\"`" + `
}
type Output struct {
 Data []*Row ` + "`parameter:\"Data,kind=output,in=view\" view:\"rows,type=Row\" sql:\"SELECT id FROM records\"`" + `
}
type Row struct { ID int ` + "`sqlx:\"id\"`" + ` }
type Handler struct{}
func NewConvert() handler.Contract[Input,Output] { return &Handler{} }
func (*Handler) Exec(context.Context,handler.Session,*Input,*Output)error{return nil}
`
	writeSourceHandlerFile(t, root, "business/contracts.go", business)
	source.Text = fmt.Sprintf(`#package(%q)
#import('business',%q)
#setting($_ = $handler_factory('business.NewConvert','Convert'))
#setting($_ = $route('/convert','GET'))
#setting($_ = $input_type('business.Input'))
#setting($_ = $output_type('business.Output'))
#setting($_ = $mcp('Convert','Custom transport'))
#define($_ = $Fields<[]string>(query/fields))
#define($_ = $Page<int>(query/page).QuerySelector('rows'))
#define($_ = $Limit<int>(query/limit).QuerySelector('rows'))`, handlerFixtureModule+"/registration", handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "dql/convert.dql", source.Text)
	var before map[string]string
	for pass := 0; pass < 2; pass++ {
		project, err := (&Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, GoBuild: source.GoBuild}).Compile(t.Context())
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		proof := project.Components[0].ExternalHandler.SourceSideParameters
		require.Len(t, proof, 2, "present direct field must not become missing-side evidence")
		generated, err := (Generator{Operation: "handler"}).Generate(t.Context(), GenerationRequest{Compiled: project.Components[0], Destination: root})
		require.NoError(t, err)
		require.Len(t, generated.Result.Plan.QuerySelectors, 2)
		if pass == 0 {
			before = sourceHandlerSnapshot(t, root)
		} else {
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		}
		actual, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
		require.NoError(t, err)
		require.Equal(t, business, string(actual))
		_, err = os.Stat(filepath.Join(root, "registration/input.go"))
		require.True(t, os.IsNotExist(err))
	}
	loaded, err := (&Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/registration"}, GoBuild: source.GoBuild}).Compile(t.Context())
	require.NoError(t, err)
	require.Len(t, loaded.Components, 1)
	require.NotNil(t, loaded.Components[0].Component.SelectorCompanion)
}
