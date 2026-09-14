package compiler

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/viant/datly/data"
	dsql "github.com/viant/datly/sql"
)

func resolveViewResources(root *data.View, resources fs.FS) error {
	visited := map[*data.View]bool{}
	var resolve func(*data.View) error
	resolve = func(view *data.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		if view.Spec.Source != nil && (len(view.Spec.Source.Embeds) > 0 ||
			(strings.TrimSpace(view.Spec.Source.SQL) == "" && strings.TrimSpace(view.Spec.Source.URI) != "")) {
			if resources == nil {
				return fmt.Errorf("resource filesystem is required for view %q", view.Spec.Name)
			}
			if err := dsql.ResolveSource(view.Spec.Name, view.Spec.Source, resources); err != nil {
				return err
			}
		}
		for _, relation := range view.Relations {
			if relation == nil || relation.Of == nil {
				continue
			}
			if err := resolve(relation.Of.View); err != nil {
				return err
			}
		}
		return nil
	}
	return resolve(root)
}
