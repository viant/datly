package index

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestIndexedCubeOutputAliasResolvesUnderlyingWarmupShape(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, root)
	writeFixture(t, root, "api/holder.go", `package api
import xdatly "github.com/viant/xdatly"
type Input struct{}
type Output struct{ Data []Row `+"`"+`view:"rows,cacheWarmup=warm"`+"`"+` }
type Row struct{ ID int }
type CubeOutput = Output
type Cube struct{ Contract xdatly.Component[Input,CubeOutput] `+"`"+`component:"RowsCube,path=/rows/cube,method=POST"`+"`"+` }
`)
	snapshot, err := (Builder{Config: Config{BaseDir: root, Include: []string{"example.com/app/api"}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, _, _, found := snapshot.Route("POST", "/rows/cube")
	if !found || !entry.Warmup {
		t.Fatalf("aliased cube output lost indexed route/warmup shape: %+v", entry)
	}
}
