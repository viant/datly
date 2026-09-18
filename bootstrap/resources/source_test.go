package resources

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSourceResourcesDiscoversGeneratedEmbedCapability(t *testing.T) {
	root := t.TempDir()
	source := `package sample
import "embed"
const ReportDatlyResourceNamespace = "sample_report"
//go:embed "sql/report.sql" "sql/keys.sql"
var ReportDatlyResources embed.FS
type EmbedFS embed.FS
var EmbedFs *EmbedFS
func (*EmbedFS) EmbedFS() *embed.FS { return &ReportDatlyResources }
`
	if err := os.WriteFile(filepath.Join(root, "resources.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	actual, err := sourceResources(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(actual) != 1 || actual[0].Namespace != "sample_report" || !reflect.DeepEqual(actual[0].Files, []string{"sql/report.sql", "sql/keys.sql"}) {
		t.Fatalf("resources = %#v", actual)
	}
}
