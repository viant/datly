package generate

import (
	"fmt"

	"github.com/viant/datly/spec"
)

// PackageOwnership reads the destination recorded by generation for a package
// component. Type lookup context alone does not establish a destination.
type PackageOwnership struct{ Directory string }

func (p PackageOwnership) Destination(key spec.Key) (string, error) {
	manifest, err := readScaffoldMetadata(p.Directory)
	if err != nil {
		return "", err
	}
	// A resource-only descriptor carries no generation destination authority.
	// Leave resource validation to the shared package-asset loader.
	if manifest.resourceOnly() {
		return "", nil
	}
	if manifest.exists {
		if err := manifest.validateVersion(); err != nil {
			return "", err
		}
	}
	manifest, err = manifest.forOwner(key.Name, true)
	if err != nil {
		return "", err
	}
	if !manifest.exists || manifest.Identity != key.String() || manifest.ComponentPackage == "" {
		return "", nil
	}
	if manifest.ComponentPackage != key.Scope {
		return "", fmt.Errorf("generated component %s ownership does not match package %s", key.String(), key.Scope)
	}
	return manifest.ComponentPackage, nil
}

func (m *scaffoldManifest) resourceOnly() bool {
	if m == nil || m.Version != 0 || m.Identity != "" || m.ComponentPackage != "" ||
		m.Owner != "" || len(m.Destinations) != 0 || len(m.Files) != 0 ||
		len(m.Roles) != 0 || len(m.Fingerprints) != 0 || len(m.ProjectionFields) != 0 {
		return false
	}
	if m.Resources == nil && len(m.Owners) == 0 {
		return false
	}
	for _, owner := range m.Owners {
		if !owner.resourceOnly() {
			return false
		}
	}
	return true
}
