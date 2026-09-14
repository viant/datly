package registry

import "github.com/viant/datly/runtime/output"

// OutputContract is the immutable encoding plan published with a component.
// Encoding ownership remains target-neutral; HTTP only selects the format.
type OutputContract = output.Plan
