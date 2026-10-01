package reader

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
)

func (s *Session) rowLock(view *data.View) (bool, error) {
	if len(s.ForUpdate) == 0 {
		return false, nil
	}
	locked := false
	seen := map[*data.View]bool{}
	for _, name := range s.ForUpdate {
		if strings.TrimSpace(name) == "" {
			return false, fmt.Errorf("row lock view name is required")
		}
		var target *data.View
		if name == dexec.RootView {
			target = s.Artifact.Root.View
		} else {
			var err error
			target, err = s.Artifact.ViewIndex.Resolve(name)
			if err != nil {
				return false, fmt.Errorf("row lock view: %w", err)
			}
		}
		if seen[target] {
			return false, fmt.Errorf("row lock view %q requested twice", name)
		}
		seen[target] = true
		if target == nil || target.Spec.RowLock == "" {
			return false, fmt.Errorf("row lock view %q has no declared capability", name)
		}
		if target.Spec.InMemory {
			return false, fmt.Errorf("row lock view %q is an in-memory result, not a physical source", name)
		}
		if planned := s.Artifact.views[target]; planned != nil && planned.Partitioner != nil {
			return false, fmt.Errorf("row lock view %q uses a partitioned physical source", name)
		}
		if target == view {
			locked = true
		}
	}
	return locked, nil
}
