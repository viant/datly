package codec

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"time"
)

const (
	KeyTimeDiff = "TimeDiff"
)

type (
	TimeDiffFactory struct{}

	TimeDiff struct {
		fromField  string
		toField    string
		outputUnit string
	}

	timeExtractor struct {
		reflect.Type
		fromField      *xunsafe.Field
		isFromFieldPtr bool
		toField        *xunsafe.Field
		isToFieldPtr   bool
	}
)

func (e *timeExtractor) init(rType reflect.Type, from, to string) error {
	e.Type = types.EnsureStruct(rType)
	e.fromField = xunsafe.FieldByName(e.Type, from)
	if e.fromField == nil {
		return fmt.Errorf("unable to locate  %s in %s", e.fromField, e.Type.String())
	}
	e.isFromFieldPtr = e.fromField.Kind() == reflect.Ptr
	if types.EnsureStruct(e.fromField.Type) != xreflect.TimeType {
		return fmt.Errorf("invalid field type expected %s, but had %s", xreflect.TimeType.String(), e.fromField.Type.Name())

	}
	e.toField = xunsafe.FieldByName(e.Type, to)
	if e.toField == nil {
		return fmt.Errorf("unable to locate  %s in %s", e.toField, e.Type.String())
	}
	e.isToFieldPtr = e.toField.Kind() == reflect.Ptr
	return nil

}

func (e *timeExtractor) getTime(holder interface{}, useFrom bool) *time.Time {
	ptr := xunsafe.AsPointer(holder)
	aField := e.toField
	isPtr := e.isToFieldPtr
	if useFrom {
		aField = e.fromField
		isPtr = e.isFromFieldPtr
	}
	if isPtr {
		return aField.TimePtr(ptr)

	}
	t := aField.Time(ptr)
	return &t
}

func (e *TimeDiffFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if err := ValidateMinArgs(codecConfig, KeyTimeDiff, 3); err != nil {
		return nil, err
	}

	outputUnit := strings.ToLower(codecConfig.Args[2])

	switch outputUnit {
	case "ms", "sec", "min", "hour", "day":
	default:
		return nil, fmt.Errorf(`unsupported time unit, use one of the following: "ms","sec","min","hour", "day"`)
	}

	ret := &TimeDiff{
		fromField:  codecConfig.Args[0],
		toField:    codecConfig.Args[1],
		outputUnit: outputUnit,
	}
	return ret, nil
}

func (u *TimeDiff) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return xreflect.IntType, nil
}

func (u *TimeDiff) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)

	extractor := &timeExtractor{}
	if err := extractor.init(reflect.TypeOf(raw), u.fromField, u.toField); err != nil {
		return nil, err
	}

	from := extractor.getTime(raw, false)
	if from == nil {
		return 0, nil
	}
	to := extractor.getTime(raw, true)
	if to == nil {
		return 0, nil
	}
	diff := from.Sub(*to)
	switch u.outputUnit {
	case "ms":
		return int(diff.Milliseconds()), nil
	case "sec":
		return int(diff.Seconds()), nil
	case "min":
		return int(diff.Minutes()), nil
	case "hour":
		return int(diff.Hours()), nil
	case "day":
		fromDays := normalizedDays(from)
		toDays := normalizedDays(to)
		if fromDays < toDays {
			return 0, nil
		}
		return (fromDays - toDays) + 1, nil
	}
	return 0, nil

}

func normalizedDays(t *time.Time) int {
	y, m, d := t.Date()
	return 366*y + 31*int(m) + d
}
