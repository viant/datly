package scan

import "github.com/viant/datly/repository/shape"

// ShapeSpecKind implements shape.ScanSpec.
func (r *Result) ShapeSpecKind() string { return "scan" }

// DescriptorsFrom extracts the typed scan result from a ScanResult.
// Returns (nil, false) when a is nil or contains an unexpected concrete type.
func DescriptorsFrom(a *shape.ScanResult) (*Result, bool) {
	if a == nil {
		return nil, false
	}
	r, ok := a.Descriptors.(*Result)
	return r, ok && r != nil
}
