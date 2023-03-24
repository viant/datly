package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal/default"
	"github.com/viant/xunsafe"
	"strconv"
	"time"
	"unsafe"
)

type timePtrMarshaller struct {
	timeFormat string
	zeroValue  string
	tag        *DefaultTag
}

func newTimePtrMarshaller(tag *DefaultTag, config _default.Default) *timePtrMarshaller {
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

	return &timePtrMarshaller{
		timeFormat: timeFormat,
		zeroValue:  zeroString,
		tag:        tag,
	}
}

func (t *timePtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	aTime := xunsafe.AsTimeAddrPtr(pointer)

	timeDst := time.Time{}
	if err := decoder.AddTime(&timeDst, t.timeFormat); err != nil {
		return err
	}

	if !timeDst.IsZero() {
		*aTime = &timeDst
	}

	return nil
}

func (t *timePtrMarshaller) MarshallObject(ptr unsafe.Pointer, sb *MarshallSession) error {
	aTime := xunsafe.AsTimeAddrPtr(ptr)
	if aTime == nil || *aTime == nil {
		sb.WriteString(t.zeroValue)
		return nil
	}

	return appendTime(sb, **aTime, t.timeFormat)
}
