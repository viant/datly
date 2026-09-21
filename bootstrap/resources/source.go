package resources

import "github.com/viant/datly/internal/packageasset"

// sourceResources delegates Go-source inspection to the package-asset owner.
// Bootstrap consumes the resulting manifest and does not own Go syntax.
func sourceResources(directory string) ([]*packageasset.Resources, error) {
	return packageasset.DiscoverSourceResources(directory)
}
