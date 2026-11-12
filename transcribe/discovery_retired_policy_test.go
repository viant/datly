package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func TestDiscoveryCurrentDQLReplacesRetiredGeneratedViewPolicy(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	text := `#package('records')
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Rows<[]*Row>(body/data))
#define($_ = $Data<[]*Row>(output/body))
#define($_ = $Current<[]*Row>(view/Current) /* SELECT r.*,type(r,'Row') FROM records r */)
SELECT r.*,c.*,type(r,'Row'),type(c,'Child'),lifecycle_type(r,'RowLifecycle')
FROM (${embed:sql/records.sql}) r JOIN (children) c ON c.record_id=r.id`
	require.NoError(t, os.MkdirAll(filepath.Join(root, "source/sql"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/sql/records.sql"), []byte("SELECT id,name FROM (records)"), 0600))
	resources, err := resource.New().WithDefault(os.DirFS(filepath.Join(root, "source")))
	require.NoError(t, err)
	prepared := func(_ context.Context, request column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
		if request.Name == "c" {
			return &column.PreparedColumnResult{Table: "children", Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true}, {Name: "record_id", Source: "record_id", Type: spec.TypeRef{Name: "int"}}}}, nil
		}
		return &column.PreparedColumnResult{Table: "records", Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true}, {Name: "name", Source: "name", Type: spec.TypeRef{Name: "string"}}}}, nil
	}
	generator := Generator{Operation: "patch"}
	_, err = generator.Generate(ctx, GenerationRequest{Source: &Source{Name: "Records", Scope: "example.com/generated/source", Text: text, Resources: resources, PreparedColumns: prepared}, Destination: root})
	require.NoError(t, err)
	input := filepath.Join(root, "records/input.go")
	data, err := os.ReadFile(input)
	require.NoError(t, err)
	require.Contains(t, string(data), "auxiliary=true")
	rowPath := filepath.Join(root, "records/views.go")
	rowBefore, err := os.ReadFile(rowPath)
	require.NoError(t, err)
	stale := strings.Replace(string(data), "auxiliary=true", "auxiliary=true,rootNullPolicy=skip-auxiliary", 1)
	require.NoError(t, os.WriteFile(input, []byte(stale), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "records/methods.go"), []byte("package records\nfunc (*Row) Existing() string {return \"kept\"}\n"), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "source"), 0700))
	text = strings.Replace(text, "#package('records')", "#package('example.com/generated/records')", 1)
	text = strings.Replace(text, "#setting($_ = $route", "#import('self','example.com/generated/records')\n#setting($_ = $route", 1)
	text = strings.Replace(text, "$Current<[]*Row>(view/Current)", "$Current<[]*Row>(view/Current).WithTypeName('self.Row')", 1)
	text = strings.Replace(text, "SELECT r.*,type(r,'Row') FROM records r", "SELECT id,name FROM records", 1)
	path := filepath.Join(root, "source/Records.dql")
	require.NoError(t, os.WriteFile(path, []byte(text), 0600))
	discovery := &Discovery{BaseDir: root, PreparedColumns: prepared}
	_, err = (&Discovery{BaseDir: root, Include: []string{"example.com/generated/records"}, PreparedColumns: prepared}).Compile(ctx)
	require.ErrorContains(t, err, "rootNullPolicy requires")
	undeclared := strings.Replace(stale, "type Input struct {", "type Input struct {\n Unchanged []*Row `parameter:\"Unchanged,kind=view,in=Unchanged\" view:\"Unchanged,table=records,rootNullPolicy=skip-auxiliary\"`\n", 1)
	require.NotEqual(t, stale, undeclared)
	require.NoError(t, os.WriteFile(input, []byte(undeclared), 0600))
	_, err = discovery.TranscribeBatch(ctx, BatchRequest{Generator: generator, Destination: root, Rules: []BatchRule{{Source: path}}})
	require.ErrorContains(t, err, "rootNullPolicy requires")
	require.NoError(t, os.WriteFile(input, []byte(stale), 0600))
	for _, conflict := range []string{strings.Replace(text, "'/records'", "'/other'", 1), strings.Replace(text, "$input_type('Input')", "$input_type('Output')", 1)} {
		require.NoError(t, os.WriteFile(path, []byte(conflict), 0600))
		_, err = discovery.TranscribeBatch(ctx, BatchRequest{Generator: generator, Destination: root, Rules: []BatchRule{{Source: path}}})
		require.ErrorContains(t, err, "conflicting component route or input/output contract authority")
		unchanged, readErr := os.ReadFile(input)
		require.NoError(t, readErr)
		require.Equal(t, stale, string(unchanged))
	}
	badViewType := strings.Replace(text, "WithTypeName('self.Row')", "WithTypeName('self.Child')", 1)
	require.NoError(t, os.WriteFile(path, []byte(badViewType), 0600))
	_, err = discovery.TranscribeBatch(ctx, BatchRequest{Generator: generator, Destination: root, Rules: []BatchRule{{Source: path}}})
	require.ErrorContains(t, err, "does not match package type")
	unchanged, readErr := os.ReadFile(input)
	require.NoError(t, readErr)
	require.Equal(t, stale, string(unchanged))
	require.NoError(t, os.WriteFile(path, []byte(text), 0600))
	for range 2 {
		_, err = discovery.TranscribeBatch(ctx, BatchRequest{Generator: generator, Destination: root, Rules: []BatchRule{{Source: path}}})
		require.NoError(t, err)
		current, readErr := os.ReadFile(input)
		require.NoError(t, readErr)
		require.NotContains(t, string(current), "skip-auxiliary")
		require.Equal(t, string(data), string(current), "binding, view tags and input identity changed")
		rowAfter, readErr := os.ReadFile(rowPath)
		require.NoError(t, readErr)
		require.Equal(t, string(rowBefore), string(rowAfter), "row identities, tags or relations changed")
		sourceSQL, readErr := os.ReadFile(filepath.Join(root, "source/sql/records.sql"))
		require.NoError(t, readErr)
		require.Equal(t, "SELECT id,name FROM (records)", string(sourceSQL))
		require.Contains(t, string(current), "auxiliary=true")
		methods, readErr := os.ReadFile(filepath.Join(root, "records/methods.go"))
		require.NoError(t, readErr)
		require.Contains(t, string(methods), "Existing")
	}
	command := exec.Command("go", "test", "-mod=mod", "./records")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := command.CombinedOutput()
	require.NoError(t, err, string(out))
}
