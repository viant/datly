package output

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/structology/encoding/jsontab"
	"github.com/viant/xlsy"
	"github.com/viant/xmlify"
)

type cachedCSV struct {
	once    sync.Once
	encoder *csv.Marshaller
	err     error
}
type cachedXML struct {
	once    sync.Once
	encoder *xmlify.Marshaller
	err     error
}

func (p *Plan) marshalCSV(value any) ([]byte, error) {
	if p.rows == nil {
		return nil, fmt.Errorf("CSV output requires typed rows")
	}
	rows, err := p.rows.value(value)
	if err != nil {
		return nil, err
	}
	if p.rowPresentation != nil {
		rows, err = p.rowPresentation.value(rows)
		if err != nil {
			return nil, err
		}
	}
	p.csvEncoder.once.Do(func() {
		p.csvEncoder.encoder, p.csvEncoder.err = csv.NewMarshaller(rows.Type(), &csv.Config{FieldSeparator: ",", ObjectSeparator: "\n", EncloseBy: `"`, EscapeBy: "\\", NullValue: "null"})
	})
	if p.csvEncoder.err != nil {
		return nil, p.csvEncoder.err
	}
	return p.csvEncoder.encoder.Marshal(rows.Interface())
}

func (p *Plan) marshalXML(value any) ([]byte, error) {
	var err error
	value, err = p.present(value)
	if err != nil {
		return nil, err
	}
	entry, _ := p.xmlEncoders.LoadOrStore(reflect.TypeOf(value), &cachedXML{})
	cached := entry.(*cachedXML)
	cached.once.Do(func() {
		cached.encoder, cached.err = xmlify.NewMarshaller(reflect.TypeOf(value), &xmlify.Config{PreserveEmptyHolders: true, RegularRootTag: "result", RegularRowTag: "row", RegularNullValue: `nil="true"`})
	})
	if cached.err != nil {
		return nil, cached.err
	}
	return cached.encoder.Marshal(value)
}

func (p *Plan) marshalXLS(value any) ([]byte, error) {
	var err error
	value, err = p.present(value)
	if err != nil {
		return nil, err
	}
	return xlsy.NewMarshaller().Marshal(value)
}

func (p *Plan) present(value any) (any, error) {
	if p.presentation == nil {
		return value, nil
	}
	source := reflect.ValueOf(value)
	if source.Kind() == reflect.Pointer && p.presentation.source.Kind() != reflect.Pointer {
		source = source.Elem()
	}
	projected, err := p.presentation.value(source)
	if err != nil {
		return nil, err
	}
	return projected.Interface(), nil
}

func (p *Plan) marshalTabular(ctx context.Context, value any) ([]byte, error) {
	if p.rows == nil {
		return nil, fmt.Errorf("tabular output requires typed rows")
	}
	rows, err := p.rows.value(value)
	if err != nil {
		return nil, err
	}
	if p.rowPresentation != nil {
		rows, err = p.rowPresentation.value(rows)
		if err != nil {
			return nil, err
		}
	}
	var options []jsontab.Option
	if p.caseFormat != "" {
		options = append(options, jsontab.WithCaseFormat(p.caseFormat))
	}
	if p.timeLayout != "" {
		options = append(options, jsontab.WithTimeLayout(p.timeLayout))
	}
	encoded, err := jsontab.MarshalContext(ctx, rows.Interface(), options...)
	if err != nil {
		return nil, err
	}
	if p.rows.envelope == nil {
		return encoded, nil
	}
	original := reflect.ValueOf(value)
	for original.Kind() == reflect.Pointer {
		original = original.Elem()
	}
	envelope := reflect.New(p.rows.envelope).Elem()
	for _, field := range p.rows.fields {
		if !field.Exported || len(field.Index) != 1 {
			continue
		}
		destination := envelope.FieldByName(field.Name)
		if field.Name == p.rows.name {
			destination.Set(reflect.ValueOf(json.RawMessage(encoded)))
		} else {
			destination.Set(original.FieldByIndex(field.Index))
		}
	}
	return p.marshalJSON(ctx, envelope.Interface(), false)
}
