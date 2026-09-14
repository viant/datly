package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

func mergeOutputRelations(root *spec.View, authored []*spec.Relation) (*spec.View, error) {
	if len(authored) == 0 {
		return root, nil
	}
	if root == nil {
		return nil, fmt.Errorf("output relations require a root view")
	}
	for _, relation := range authored {
		if relation == nil {
			continue
		}
		for _, existing := range root.Relations {
			if existing == nil || !strings.EqualFold(strings.TrimSpace(existing.Holder), strings.TrimSpace(relation.Holder)) {
				continue
			}
			return nil, fmt.Errorf("output relation holder %q is declared more than once", relation.Holder)
		}
		root.Relations = append(root.Relations, relation)
	}
	return root, nil
}
