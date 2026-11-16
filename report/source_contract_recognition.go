package report

import (
	"reflect"

	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

// selectorRecognition starts from the selected contract. Only source-protected
// selector leaves recover their original explicit MCP spellings, so the single
// native name normalizer can retain their canonical identity for admission.
// Public siblings keep selected names; removed aliases never regain authority.
func selectorRecognition(walker *shape.RewritePlan, selected *shape.Representation, paths map[string]string, visible map[string]bool) (*shape.Representation, error) {
	protected := false
	for _, name := range paths {
		protected = protected || !visible[name]
	}
	if !protected {
		return nil, nil
	}
	return walker.RewriteRepresentation(func(path string, parent reflect.Type, original *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		current := *original
		if selected != nil {
			metadata, err := selected.Lookup(path)
			if err != nil {
				return nil, err
			}
			for metadata.Original().Kind() == reflect.Pointer {
				metadata = metadata.Elem()
			}
			field, _ := parent.FieldByName(original.Name)
			effective := metadata.Field(field.Index[0])
			current.Name, current.Tag = effective.Name, effective.Tag
		}
		if source, ok := paths[joinContractPath(path, original.Name)]; ok && !visible[source] {
			restored := (&xcontract.Field{Name: current.Name, Tag: current.Tag}).SetTag("mcp", original.Tag.Get("mcp"))
			current.Tag = restored.Tag
		}
		return &current, nil
	})
}
