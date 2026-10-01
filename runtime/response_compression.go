package runtime

import "github.com/viant/datly/spec"

// ResponseCompressionByRoute returns isolated public-route transport metadata
// without loading a lazy component or changing request execution/output lookup.
func (r *Runtime) ResponseCompressionByRoute(method, path string) *spec.ResponseCompression {
	if r == nil {
		return nil
	}
	component, _, ok := r.publicComponentByRoute(method, path)
	if !ok || component == nil || component.Settings == nil {
		return nil
	}
	return component.Settings.ResponseCompression.Clone()
}
