package typecatalog

import "fmt"

// ImportReferences adds only the requested declarations from a staged catalog,
// preserving their ownership. It does not replace whole package snapshots.
func (c *Catalog) ImportReferences(source *Catalog, keys []string) error {
	if c == nil || source == nil {
		return fmt.Errorf("source and destination catalogs are required")
	}
	source.mu.RLock()
	selected := map[string][]registration{}
	for _, key := range keys {
		for _, entry := range source.items[key] {
			typ, err := CloneDescriptor(entry.Type)
			if err != nil {
				source.mu.RUnlock()
				return err
			}
			selected[key] = append(selected[key], registration{Origin: entry.Origin, Type: typ})
		}
	}
	source.mu.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	updates := map[string][]registration{}
	for key, entries := range selected {
		current := append([]registration(nil), c.items[key]...)
		for _, entry := range entries {
			found := false
			for _, prior := range current {
				if prior.Origin != entry.Origin {
					continue
				}
				if !equalType(prior.Type, entry.Type) {
					return fmt.Errorf("type %q is already registered from origin %q", key, entry.Origin)
				}
				found = true
			}
			if !found {
				current = append(current, entry)
			}
		}
		updates[key] = current
	}
	for key, entries := range updates {
		c.items[key] = entries
	}
	return nil
}
