package genpatch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// NamedSplitIdentityDQL keeps all request/current/key plumbing generated.
var NamedSplitIdentityDQL = strings.NewReplacer(PackageDirective, PackageDirective+`
#import('requests','github.com/viant/datly/genfixture/requests')
#import('responses','github.com/viant/datly/genfixture/responses')
#import('rows','github.com/viant/datly/genfixture/entities')
#import('items','github.com/viant/datly/genfixture/items')
#import('details','github.com/viant/datly/genfixture/details')
#setting($_ = $input_type('requests.OrdersInput'))
#setting($_ = $output_type('responses.OrdersOutput'))
`, "SELECT o.*, Items.*, Details.*, Kinds.*,", "SELECT o.*, Items.*, Details.*, Kinds.*,type(o,'rows.Order'),type(Items,'items.Item'),type(Details,'details.Detail'),").Replace(NamedIdentityDQL)

// SplitResolvedRuntime authors Input.Init in its real input package, leaving
// handler/entity destinations independent and without callback registration.
func SplitResolvedRuntime(t testing.TB, root string) string {
	t.Helper()
	source := ResolvedIdentityRuntime(true)
	start, end := strings.Index(source, "var lookupRead bool"), strings.Index(source, "func TestGeneratedPatchRuntime")
	initializer := strings.ReplaceAll(source[start:end], "lookupRead", "LookupRead")
	if err := os.WriteFile(filepath.Join(root, "requests", "init.go"), []byte("package requests\nimport(\"context\";\"fmt\";\"reflect\")\n"+initializer), 0644); err != nil {
		t.Fatal(err)
	}
	source = source[:start] + source[end:]
	source = strings.Replace(source, ` "context"`, ` "context";requests "github.com/viant/datly/genfixture/requests";rows "github.com/viant/datly/genfixture/entities"`, 1)
	source = strings.Replace(source, ` "fmt"`, "", 1)
	return strings.NewReplacer("OrdersViewHas", "rows.OrderHas", "OrdersView", "rows.Order", "lookupRead", "requests.LookupRead").Replace(source)
}
