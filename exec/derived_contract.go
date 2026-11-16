package exec

import (
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"reflect"
)

// DerivedContract is registration-owned provenance for a native report facade.
// Runtime selects its exact source in each operation; this is never authority.
type DerivedContract interface {
	SourceContractTarget() ComponentTarget
	PrepareSourceContract(*spec.Component, reflect.Type, reflect.Type) (func(*EffectiveContract) (*ContractProjection, error), error)
}

// ContractProjection is operation-local preparation over selected source
// metadata. Only Prepare's immutable result may enter the existing variant.
// Neither callback nor source binding state is retained by that variant.
type ContractProjection struct {
	Policy  *xcontract.Policy
	Prepare func(*shape.Representation) (any, error)
}

// ContractInputNormalization is immutable recognition metadata for native name
// normalization. Publication and decoding keep the selected input contract.
type ContractInputNormalization interface{ ContractInputNormalization() *shape.Representation }
