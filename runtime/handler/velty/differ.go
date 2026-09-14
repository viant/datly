package velty

import (
	"context"
	"fmt"
	xdiffer "github.com/viant/xdatly/differ"
)

// Differ adapts the scoped comparison capability to authored Velty calls.
type Differ struct {
	ctx     context.Context
	service xdiffer.Differ
}

// Diff retains the original Velty differ's set-marker filtering policy.
func (d *Differ) Diff(from, to any) (*xdiffer.ChangeLog, error) {
	if d == nil || d.service == nil {
		return nil, fmt.Errorf("velty differ capability is not configured")
	}
	return d.service.Diff(d.ctx, from, to, xdiffer.WithSetMarker(true))
}
