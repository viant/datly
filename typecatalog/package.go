package typecatalog

import (
	"fmt"
	"strings"

	x "github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

// RegisterPackage adds every declared type in a loaded synthetic package under
// one origin. The package remains the source artifact; Catalog is the sole
// authority used for subsequent type resolution.
func (c *Catalog) RegisterPackage(origin TypeOrigin, pkg *smodel.Package) error {
	if c == nil {
		return fmt.Errorf("type catalog is required")
	}
	if !validTypeOrigin(origin) {
		return fmt.Errorf("unknown type origin %q", origin)
	}
	if pkg == nil {
		return fmt.Errorf("synthetic package is required")
	}
	packagePath := strings.TrimSpace(pkg.PkgPath)
	if packagePath == "" {
		return fmt.Errorf("synthetic package path is required")
	}
	types := make([]*x.Type, 0, len(pkg.Types))
	keys := make(map[string]bool, len(pkg.Types))
	for _, declared := range pkg.Types {
		if declared == nil {
			continue
		}
		name := strings.TrimSpace(declared.Name)
		if name == "" {
			return fmt.Errorf("synthetic type name is required in package %q", packagePath)
		}
		typePackage := strings.TrimSpace(declared.PkgPath)
		if typePackage == "" {
			typePackage = packagePath
		}
		if typePackage != packagePath {
			return fmt.Errorf("synthetic type %q belongs to package %q, not %q", name, typePackage, packagePath)
		}
		var typ *x.Type
		registered, err := (x.Cloner{}).Synthetic(declared)
		if err != nil {
			return err
		}
		if registered.ReflectType != nil {
			typ = x.NewType(registered.ReflectType,
				x.WithPkgPath(typePackage),
				x.WithName(name),
				x.WithSyntheticType(registered),
			)
		} else {
			typ = &x.Type{PkgPath: typePackage, Name: name, SynteticType: registered}
		}
		key := typ.Key()
		if keys[key] {
			return fmt.Errorf("synthetic package %q declares type %q more than once", packagePath, name)
		}
		keys[key] = true
		types = append(types, typ)
	}

	// Validate the complete package before replacing this origin's entries.
	// Existing snapshots retain their prior x.Type pointers.
	c.mu.Lock()
	defer c.mu.Unlock()
	updates := make(map[string][]registration, len(types))
	for key, registrations := range c.items {
		if keys[key] {
			continue
		}
		filtered := append([]registration(nil), registrations...)
		filtered = filtered[:0]
		for _, existing := range registrations {
			if existing.Origin == origin && existing.Type != nil && existing.Type.PkgPath == packagePath {
				continue
			}
			filtered = append(filtered, existing)
		}
		if len(filtered) != len(registrations) {
			updates[key] = filtered
		}
	}
	for _, typ := range types {
		key := typ.Key()
		registrations := append([]registration(nil), c.items[key]...)
		filtered := registrations[:0]
		for _, existing := range registrations {
			if existing.Origin != origin {
				filtered = append(filtered, existing)
			}
		}
		detached, err := (x.Cloner{}).Type(typ)
		if err != nil {
			return err
		}
		// Resolve the native lazy identity cache before publishing immutable storage.
		_ = detached.Key()
		updates[key] = append(filtered, registration{Origin: origin, Type: detached})
	}
	for key, registrations := range updates {
		if len(registrations) == 0 {
			delete(c.items, key)
		} else {
			c.items[key] = registrations
		}
	}
	return nil
}
