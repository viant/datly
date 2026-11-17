package output

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	"github.com/viant/tagly/format/text"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"reflect"
	"strings"
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
	result, err := p.compiler.compile(input, false)
	if err != nil {
		return nil, err
	}
	result.effectiveSource = p.typeOf
	if p.effectiveSource != nil {
		result.effectiveSource = p.effectiveSource
	}
	result.prepareFormats()
	if err := result.prepareSelectorOutputRoot(); err != nil {
		return nil, err
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
	if metadata == nil || !metadata.Changed() && (p == nil || p.formatPlan == nil) {
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
	result, err := p.compiler.compile(p.compileInput, false)
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
	result.prepareFormats()
	if err := result.prepareSelectorOutputRoot(); err != nil {
		return nil, err
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

// WithCaseFormat compiles an immutable transport-local presentation policy.
// Original compiler lookup, row selection, effective storage and explicit field
// metadata remain authoritative. Opaque/custom-owned bytes are never rewritten.
func (p *Plan) WithCaseFormat(format string) (*Plan, error) {
	if p == nil || strings.TrimSpace(format) == "" {
		return p, nil
	}
	if !text.NewCaseFormat(format).IsDefined() {
		return nil, fmt.Errorf("invalid output case format %q", format)
	}
	if p.TransportReady() || p.custom != nil {
		return p, nil
	}
	input := p.compileInput
	if input.Component == nil {
		input.Component = &spec.Component{}
	} else {
		input.Component = input.Component.Clone()
	}
	if input.Component.Settings == nil {
		input.Component.Settings = &spec.Settings{}
	}
	input.Component.Settings.CaseFormat = format
	result, err := p.compiler.Compile(input)
	if err != nil {
		return nil, err
	}
	if p.representation != nil {
		result, err = result.WithRepresentation(p.representation)
		if err != nil {
			return nil, err
		}
	}
	result.effectiveSource = p.effectiveSource
	if result.effectiveSource != nil {
		result.prepareFormats()
	}
	return result, nil
}

// prepareFormats compiles reusable typed codec authority once per immutable variant.
func (p *Plan) prepareFormats() {
	if p.custom != nil || p.TransportReady() {
		return
	}
	original := p.typeOf
	if p.effectiveSource != nil {
		original = p.effectiveSource
	}
	if original != nil && (original.Implements(reflect.TypeFor[json.Marshaler]()) || reflect.PointerTo(original).Implements(reflect.TypeFor[json.Marshaler]()) || original.Implements(reflect.TypeFor[JSONWireType]()) || reflect.PointerTo(original).Implements(reflect.TypeFor[JSONWireType]())) {
		return
	}
	p.formatPresentation, p.formatError = (presentationCompiler{excluded: p.exclude, representation: p.representation, hideJSON: p.effectiveSource != nil, compileEnums: true, cell: p.recursiveCell, envelope: p.rows != nil && len(p.rows.index) > 0, canonical: p.effectiveSource, fieldVisible: p.formatFieldVisibility()}).compile(p.typeOf, "", nil)
	if p.formatError != nil {
		return
	}
	if p.formatPresentation == nil && p.representation == nil && p.effectiveSource == nil {
		return
	}
	typ := p.typeOf
	if p.formatPresentation != nil {
		typ = p.formatPresentation.target
	}
	p.formatPlan, p.formatError = p.presentationPlan(typ)

}

// recursiveCell prepares the same native JSON owner as the body, once within
// the immutable format variant. Recursive and opaque metadata edits are rejected upstream.
func (p *Plan) recursiveCell(typ reflect.Type, metadata *xshape.Representation) (func(context.Context, any) ([]byte, error), error) {
	if !p.transformedJSON() {
		encoder := jsonmarshal.NewStandardEncoder(nil, nil).WithRepresentation(metadata)
		return encoder.MarshalContext, nil
	}
	encoder, err := structjson.NewMarshaller(typ, p.jsonOptionsFor(metadata, nil)...)
	if err != nil {
		return nil, err
	}
	return encoder.MarshalContext, nil
}

// Native transformed JSON owns internal, presence and format visibility. Reuse
// that per-field decision before preparing an export carrier; standard JSON
// retains its own annotation semantics.
func (p *Plan) formatFieldVisibility() func(reflect.StructField, reflect.StructField, string) bool {
	if !p.transformedJSON() {
		return nil
	}
	return func(original, effective reflect.StructField, path string) bool {
		field := p.schemaFieldPresentation(original, effective, path)
		return field != nil && !field.excluded
	}
}
