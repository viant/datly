package generate

import (
	"fmt"

	"github.com/viant/datly/spec"
)

// PackageOwnership reads the destination recorded by generation for a package
// component. Type lookup context alone does not establish a destination.
type PackageOwnership struct{ Directory string }

func (p PackageOwnership) Destination(key spec.Key) (string, error) {
	manifest, err := readScaffoldManifest(p.Directory)
	if err != nil {
		return "", err
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
