package resource

import (
	"github.com/viant/mcp-protocol/server"
)

// RegisterSkills publishes validated metadata for explicitly declared roots.
// Resources must be registered first; the native registry rejects missing files.
func (c *Catalog) RegisterSkills(registry *server.Registry) error {
	for _, plan := range c.static {
		if plan.skill != nil {
			if err := registry.RegisterStaticSkill(plan.skill); err != nil {
				return err
			}
		}
	}
	return nil
}
