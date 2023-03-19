package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type TimeMarshaller struct {
	timeFormat string
	zeroValue  string
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

	zeroValue := time.Time{}
	if tag._value != nil {
		zeroValue, _ = tag._value.(time.Time)
	}

	return &TimeMarshaller{
		timeFormat: timeFormat,
		zeroValue:  strconv.Quote(zeroValue.Format(timeFormat)),
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

func (t *TimeMarshaller) MarshallObject(rType reflect.Type, ptr unsafe.Pointer, sb *Session) error {
	aTime := xunsafe.AsTime(ptr)
	if aTime.IsZero() {
		sb.WriteString(t.zeroValue)
		return nil
	}

	return appendTime(sb, aTime, t.timeFormat)
}

func appendTime(sb *Session, aTime time.Time, timeFormat string) error {
	sb.WriteByte('"')
	sb.WriteString(aTime.Format(timeFormat))
	sb.WriteByte('"')
	return nil
}
