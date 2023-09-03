package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/xunsafe"
	"strconv"
	"time"
	"unsafe"
)

type timeMarshaller struct {
	timeFormat string
	zeroValue  string
	tag        *DefaultTag
}

func newTimeMarshaller(tag *DefaultTag, config config.IOConfig) *timeMarshaller {
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

	return &timeMarshaller{
		timeFormat: timeFormat,
		zeroValue:  strconv.Quote(zeroValue.Format(timeFormat)),
		tag:        tag,
	}
}

func (t *timeMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	aTime := xunsafe.AsTimePtr(pointer)
	if err := decoder.AddTime(aTime, t.timeFormat); err != nil {
		return err
	}
	return nil
}

func (t *timeMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	aTime := xunsafe.AsTime(ptr)
	if aTime.IsZero() {
		sb.WriteString(t.zeroValue)
		return nil
	}

	return appendTime(sb, aTime, t.timeFormat)
}

func appendTime(sb *MarshallSession, aTime time.Time, timeFormat string) error {
	sb.WriteByte('"')
	sb.WriteString(aTime.Format(timeFormat))
	sb.WriteByte('"')
	return nil
}
