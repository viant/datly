package transcribe

import (
	"context"
	"strings"
	"testing"
)

func TestDiscoveryDeclaredPackageDoesNotResolveUnrelatedOutput(t *testing.T) {
	fixture := declaredPackageFixture
	text := strings.ReplaceAll(declaredPackageDQL, "#import('records','example.com/generated/api/records')\n", "")
	text = strings.ReplaceAll(text, "records.Input", "Input")
	text = strings.ReplaceAll(text, "records.Output", "Output")
	base, discovery := declaredPackageProject(t, fixture, text)
	writeSourceFile(t, base, "api/records/sibling.go", `package records
import (
 response "github.com/viant/xdatly/handler/response"
)
type SiblingOutput struct { response.Status }
`)
	writeSourceFile(t, base, "api/records/sibling_router.go", `package records
import "github.com/viant/xdatly"
type SiblingInput struct{}
type Sibling struct { Contract xdatly.Component[SiblingInput,SiblingOutput] `+"`component:\"Sibling,path=/sibling,method=GET\"`"+` }
`)
	project, err := discovery.Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 || project.Components[0].Source.Name != "Records" {
		t.Fatalf("wrong component selected: %+v", project.Components)
	}
}
