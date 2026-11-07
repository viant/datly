package output

import (
	"fmt"
	structjson "github.com/viant/structology/encoding/json"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"reflect"
	"sync"
)

// WithType recompiles effective representation using this plan's original
// presentation settings. It never mutates the shared plan or its codec caches.
func (p *Plan) WithType(typ reflect.Type) (*Plan, error) {
	if p == nil {
		return (Compiler{}).Compile(CompileInput{Type: typ})
	}
	if typ == p.typeOf {
		return p, nil
	}
	if p.TransportReady() {
		return nil, fmt.Errorf("dynamic output requires a contract-aware custom encoder")
	}
	input := p.compileInput
	input.Type = typ
	result, err := p.compiler.Compile(input)
	if err != nil {
		return nil, err
	}
	result.effectiveSource = p.typeOf
	if p.effectiveSource != nil {
		result.effectiveSource = p.effectiveSource
	}
	return result, nil
}

type valueProjectionEntry struct {
	once sync.Once
	plan *xshape.ValueProjection
	err  error
}

func (p *Plan) effectiveValue(value any) (any, error) {
	if p.effectiveSource == nil || value == nil {
		return value, nil
	}
	source := reflect.TypeOf(value)
	if source == p.typeOf {
		return value, nil
	}
	original, target := p.effectiveSource, p.typeOf
	if source == reflect.PointerTo(original) {
		target = reflect.PointerTo(target)
	} else if source != original {
		return nil, fmt.Errorf("effective output expects %s or its pointer, got %s", original, source)
	}
	stored, _ := p.projections.LoadOrStore(source, &valueProjectionEntry{})
	entry := stored.(*valueProjectionEntry)
	entry.once.Do(func() { entry.plan, entry.err = xshape.CompileProjection(source, target) })
	if entry.err != nil {
		return nil, entry.err
	}
	return entry.plan.Apply(value)
}

// WithRepresentation opts into metadata over canonical storage. Native encoders
// consume it directly. A configured legacy codec can retain its existing type
// projection when representable; otherwise it must opt into JSONEncoder.
func (p *Plan) WithRepresentation(metadata *xshape.Representation) (*Plan, error) {
	if metadata == nil || !metadata.Changed() {
		return p, nil
	}
	if p == nil || metadata.Original() != p.typeOf {
		return nil, fmt.Errorf("output representation does not match its linked type")
	}
	if p.TransportReady() {
		return nil, fmt.Errorf("dynamic output requires a contract-aware response encoder")
	}
	if p.custom != nil {
		if _, ok := p.custom.(xcontract.JSONEncoder); !ok {
			typ, err := metadata.Reflected()
			if err != nil {
				return nil, fmt.Errorf("configured JSON encoder must implement contract.JSONEncoder for this representation: %w", err)
			}
			return p.WithType(typ)
		}
	}
	result, err := p.compiler.Compile(p.compileInput)
	if err != nil {
		return nil, err
	}
	result.representation = metadata
	result.declaredTime = declaredTimeOutput(result.typeOf, metadata, map[reflect.Type]bool{})
	if result.jsonEncoder != nil || (result.custom == nil && result.declaredTime) {
		result.standardJSON = nil
		result.jsonEncoder, err = structjson.NewMarshaller(result.typeOf, result.jsonOptions()...)
		if err != nil {
			return nil, err
		}
	}
	if result.standardJSON != nil {
		result.standardJSON = result.standardJSON.WithRepresentation(metadata)
	}
	return result, nil
}

// WithContract selects opt-in metadata when present, otherwise the existing
// reflected-type path. Type and metadata must name the same canonical storage.
func (p *Plan) WithContract(typ reflect.Type, metadata *xshape.Representation) (*Plan, error) {
	if metadata == nil {
		return p.WithType(typ)
	}
	if metadata.Original() != typ {
		return nil, fmt.Errorf("effective output type and metadata disagree")
	}
	if p == nil {
		var err error
		p, err = (Compiler{}).Compile(CompileInput{Type: typ})
		if err != nil {
			return nil, err
		}
	}
	return p.WithRepresentation(metadata)
}
