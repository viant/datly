package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/xunsafe"
	"strconv"
	"time"
	"unsafe"
)

type TimePtrMarshaller struct {
	timeFormat string
	zeroValue  string
	tag        *DefaultTag
}

func NewTimePtrMarshaller(tag *DefaultTag, config marshal.Default) *TimePtrMarshaller {
	timeFormat := time.RFC3339
	if tag.Format != "" {
		timeFormat = tag.Format
	}

	if config.DateLayout != "" {
		timeFormat = config.DateLayout
	}

	var zeroValue *time.Time
	if tag._value != nil {
		zeroValue, _ = tag._value.(*time.Time)
	}

	zeroString := null
	if zeroValue != nil {
		zeroString = strconv.Quote(zeroValue.Format(timeFormat))
	}

	return &TimePtrMarshaller{
		timeFormat: timeFormat,
		zeroValue:  zeroString,
		tag:        tag,
	}
}

func (t *TimePtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, mainDecoder *gojay.Decoder, nullDecoder *gojay.Decoder) error {
	aTime := xunsafe.AsTimeAddrPtr(pointer)

	timeDst := time.Time{}
	if err := mainDecoder.AddTime(&timeDst, t.timeFormat); err != nil {
		return err
	}

	if !timeDst.IsZero() {
		*aTime = &timeDst
	}

	return nil
}

func (t *TimePtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *Session) error {
	aTime := xunsafe.AsTimeAddrPtr(ptr)
	if aTime == nil || *aTime == nil {
		sb.WriteString(t.zeroValue)
		return nil
	}

	return appendTime(sb, **aTime, t.timeFormat)
}
