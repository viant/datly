package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"time"
	"unsafe"
)

type TimeMarshaller struct {
	timeFormat string
	tag        *DefaultTag
}

func NewTimeMarshaller(tag *DefaultTag, config marshal.Default) *TimeMarshaller {
	timeFormat := time.RFC3339
	if tag.Format != "" {
		timeFormat = tag.Format
	}

	if config.DateLayout != "" {
		timeFormat = config.DateLayout
	}

	return &TimeMarshaller{
		timeFormat: timeFormat,
		tag:        tag,
	}
}

func (t *TimeMarshaller) UnmarshallObject(rType reflect.Type, pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	aTime := xunsafe.AsTimePtr(pointer)
	if err := mainDecoder.AddTime(aTime, t.timeFormat); err != nil {
		return err
	}
	return nil
}

func (t *TimeMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *bytes.Buffer, filters *Filters) error {
	aTime := xunsafe.AsTime(ptr)
	return appendTime(sb, &aTime, t.tag, t.timeFormat)
}

func appendTime(sb *bytes.Buffer, aTime *time.Time, tag *DefaultTag, timeFormat string) error {
	if (aTime == nil || aTime.IsZero()) && tag._value != nil {
		aTime = tag._value.(*time.Time)
	}

	if aTime != nil {
		sb.WriteByte('"')
		sb.WriteString(aTime.Format(timeFormat))
		sb.WriteByte('"')
		return nil
	}

	sb.WriteString(null)
	return nil
}
