package dialectcontext

import (
	"context"
	"testing"

	"github.com/viant/sqlx/metadata/info"
)

func TestDialectScopeAndExplicitClearing(t *testing.T) {
	base := context.Background()
	first, second := &info.Dialect{}, &info.Dialect{}
	parent := WithDialect(base, first)
	child := WithDialect(parent, second)
	cleared := WithDialect(parent, nil)
	if Dialect(nil) != nil || Dialect(base) != nil {
		t.Fatal("unconfigured context acquired a dialect")
	}
	if Dialect(parent) != first || Dialect(child) != second {
		t.Fatal("dialect metadata identity or scope was lost")
	}
	if Dialect(cleared) != nil || Dialect(parent) != first {
		t.Fatal("explicit clearing changed the parent scope")
	}
}
