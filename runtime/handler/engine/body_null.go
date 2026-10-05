package engine

import (
	"fmt"

	"github.com/viant/bindly"
	bodyprovider "github.com/viant/bindly/provider/body"
	"github.com/viant/datly/runtime/registry"
	xshape "github.com/viant/x/shape"
)

// normalizeBoundBodyNulls requires an actual compiled marker: markerless Go
// zero values do not establish that the caller supplied an explicit null.
// Run before supplemental binding so dependent reads see the normalized body.
func normalizeBoundBodyNulls(contract *registry.RouteInputContract, input any) error {
	if contract == nil || contract.Plan() == nil {
		return fmt.Errorf("bound input contract is required")
	}
	for _, field := range contract.Fields() {
		binding := field.Binding()
		if binding.BodyNullPolicy == "" {
			continue
		}
		accessor, err := xshape.Linked(contract.Type()).Accessor(field.Path())
		if err != nil {
			return err
		}
		value, err := accessor.Get(input)
		if err != nil {
			return err
		}
		if !value.IsNil() {
			continue
		}
		present, hasMarker, err := contract.Plan().ExplicitPresence(input, field.Path())
		if err != nil {
			return err
		}
		if !hasMarker || !present {
			if binding.Required != nil && *binding.Required {
				return &bindly.BindingError{Path: field.Path(), Code: binding.ErrorCode, Message: binding.ErrorMessage, Cause: fmt.Errorf("required bound body value %q is absent", binding.Location.In)}
			}
			continue
		}
		record, err := bodyprovider.NewEmptyRecord(field.DestinationType())
		if err != nil {
			return err
		}
		if err := accessor.Set(input, record); err != nil {
			return err
		}
	}
	return nil
}
