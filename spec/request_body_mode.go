package spec

import "fmt"

const RequestBodyOnDemand = "on_demand"

func (r *Route) ValidateRequestBodyMode() error {
	if r == nil {
		return nil
	}
	switch r.RequestBodyMode {
	case "", "eager", RequestBodyOnDemand:
		return nil
	default:
		return fmt.Errorf("unsupported request body mode %q", r.RequestBodyMode)
	}
}
