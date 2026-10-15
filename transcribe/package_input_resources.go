package transcribe

import (
	"reflect"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
)

// packageInputDefaultResources keeps Go-only imported contracts' unqualified
// resources local to the selected Input before column discovery runs.
func packageInputDefaultResources(resources *resource.Store, inputType reflect.Type, sourcePackage string) (*resource.Store, error) {
	for inputType != nil && inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return resources, nil
	}
	// A package's current source snapshot owns assets for contracts declared in
	// that same package. Imported Inputs bring their own embedded default.
	if _, exists := resources.Lookup(""); exists && inputType.PkgPath() == sourcePackage {
		return resources, nil
	}
	var embedder bootstrap.Embedder
	if value := reflect.New(inputType).Interface(); value != nil {
		embedder, _ = value.(bootstrap.Embedder)
	}
	if embedder == nil {
		if value := reflect.New(inputType).Elem().Interface(); value != nil {
			embedder, _ = value.(bootstrap.Embedder)
		}
	}
	if embedder == nil || embedder.EmbedFS() == nil {
		return resources, nil
	}
	if resources == nil {
		resources = resource.New()
	}
	return resources.WithDefault(embedder.EmbedFS())
}
