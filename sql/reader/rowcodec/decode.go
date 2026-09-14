package rowcodec

import (
	"context"
	"fmt"
	"reflect"

	sqlxio "github.com/viant/sqlx/io"
	xcodec "github.com/viant/xdatly/codec"
)

// Decoder is per-query state. SQLX owns both scan destinations and cache bytes.
type Decoder struct {
	plan         *Plan
	active       []bool
	activeCopies []bool
	newActual    func() any
	options      []xcodec.Option
	err          error
	mappedFields []sqlxio.Field
}

func (p *Plan) NewDecoder(newActual func() any, options ...xcodec.Option) *Decoder {
	if p == nil {
		return nil
	}
	return &Decoder{plan: p, active: make([]bool, len(p.bindings)), activeCopies: make([]bool, len(p.copies)), newActual: newActual, options: append([]xcodec.Option(nil), options...)}
}

func (d *Decoder) NewRow() any {
	row := reflect.New(d.plan.scanType)
	actual := d.newActual()
	value := reflect.ValueOf(actual)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Type().Elem() != d.plan.modelType {
		d.err = fmt.Errorf("codec row allocation must return *%s", d.plan.modelType)
		value = reflect.New(d.plan.modelType)
	}
	row.Elem().Field(2).Set(value)
	return row.Interface()
}

// Observe delegates field identity to SQLX's matcher; no mapping is replaced.
func (d *Decoder) Observe(columns []sqlxio.Column) error {
	clear(d.active)
	clear(d.activeCopies)
	matched, err := sqlxio.NewMatcher(nil).Match(d.plan.scanType, columns)
	if err != nil && !sqlxio.IsMatchedError(err) {
		return err
	}
	d.mappedFields = matched
	for _, field := range matched {
		if field.Field == nil {
			continue
		}
		for i, binding := range d.plan.bindings {
			if field.Field.Name == binding.rawName {
				d.active[i] = true
			}
		}
		for i, copy := range d.plan.copies {
			if field.Field.Name == copy.name {
				d.activeCopies[i] = true
			}
		}
	}
	return nil
}

// FieldIndexes translates the actual native scan mapping through this codec's
// already-compiled target accessors. Virtual Raw/Shadow wrapper fields never
// escape as model-field evidence.
func (d *Decoder) FieldIndexes() ([][]int, error) {
	var result [][]int
	for _, field := range d.mappedFields {
		index := field.FieldIndex()
		if len(index) < 2 {
			continue
		}
		var target []int
		var err error
		switch index[0] {
		case 0:
			if index[1] >= len(d.plan.bindings) {
				return nil, fmt.Errorf("invalid native codec field index")
			}
			target, err = d.plan.bindings[index[1]].target.FieldIndex()
		case 1:
			if index[1] >= len(d.plan.copies) {
				return nil, fmt.Errorf("invalid native codec shadow index")
			}
			target, err = d.plan.copies[index[1]].target.FieldIndex()
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		target = append(target, index[2:]...)
		result = append(result, target)
	}
	return result, nil
}

func (d *Decoder) Decode(ctx context.Context, row any) (any, error) {
	if d.err != nil {
		return nil, d.err
	}
	value := reflect.ValueOf(row)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Type() != d.plan.scanType {
		return nil, fmt.Errorf("invalid column codec scan row %T", row)
	}
	wrapper := value.Elem()
	actual := wrapper.Field(2).Interface()
	shadow := wrapper.Field(1)
	for i, copy := range d.plan.copies {
		if !d.activeCopies[i] && shadow.Field(copy.index).IsZero() {
			continue
		}
		if err := copy.target.Set(actual, shadow.Field(copy.index).Interface()); err != nil {
			return nil, err
		}
	}
	options := append(append([]xcodec.Option(nil), d.options...), xcodec.WithRecord(actual))
	for i, binding := range d.plan.bindings {
		if !d.active[i] {
			continue
		}
		raw := wrapper.Field(0).Field(i)
		var source any
		if binding.sourceType.Kind() == reflect.Pointer {
			source = raw.Interface()
		} else if !raw.IsNil() {
			source = raw.Elem().Interface()
		}
		var decoded any
		var err error
		// Original Datly's state.Codec.Transform preserves untyped nil sources.
		if source != nil {
			decoded, err = binding.instance.Value(ctx, source, options...)
		}
		if err != nil {
			return nil, fmt.Errorf("decode column %s: %w", binding.name, err)
		}
		if err := binding.target.Set(actual, decoded); err != nil {
			return nil, fmt.Errorf("decode column %s: %w", binding.name, err)
		}
	}
	return actual, nil
}
