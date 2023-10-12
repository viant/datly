package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	ftime "github.com/viant/structology/format/time"
	"github.com/viant/xunsafe"
	"strconv"
	"time"
	"unsafe"
)

type timeMarshaller struct {
	timeLayout string
	zeroValue  string
	tag        *DefaultTag
}

func newTimeMarshaller(tag *DefaultTag, config *config.IOConfig) *timeMarshaller {
	timeLayout := time.RFC3339
	if tag.Format != "" {
		timeLayout = tag.Format
	}
	if config.DateFormat != "" {
		config.TimeLayout = ftime.DateFormatToTimeLayout(config.DateFormat)
	}
	if config.TimeLayout != "" {
		timeLayout = config.TimeLayout
	}

	zeroValue := time.Time{}
	if tag._value != nil {
		zeroValue, _ = tag._value.(time.Time)
	}

	return &timeMarshaller{
		timeLayout: timeLayout,
		zeroValue:  strconv.Quote(zeroValue.Format(timeLayout)),
		tag:        tag,
	}
}

func (t *timeMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	aTime := xunsafe.AsTimePtr(pointer)
	if err := decoder.AddTime(aTime, t.timeLayout); err != nil {
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

	return appendTime(sb, aTime, t.timeLayout)
}

func appendTime(sb *MarshallSession, aTime time.Time, timeFormat string) error {
	sb.WriteByte('"')
	sb.WriteString(aTime.Format(timeFormat))
	sb.WriteByte('"')
	return nil
}
