package builder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/sql/locking"
)

func (o *builderOptions) applyRowLock(source string) (string, error) {
	if !o.forUpdate {
		return source, nil
	}
	if o.view == nil || strings.TrimSpace(o.view.Spec.RowLock) == "" {
		return "", fmt.Errorf("reader view does not declare row-lock capability")
	}
	return locking.ApplyOrdered(source, o.view.Spec.RowLock, o.view.Spec.RowLockOrder, o.dialect, o.transactionActive)
}
