package json

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/structology/format"
	"github.com/viant/xunsafe"
	"strconv"
	"time"
	"unsafe"
)

type timeMarshaller struct {
	timeLayout string
	zeroValue  string
	tag        *format.Tag
}

func newTimeMarshaller(tag *format.Tag, config *config.IOConfig) *timeMarshaller {
	timeLayout := time.RFC3339
	if layout := config.GetTimeLayout(); layout != "" {
		timeLayout = layout
	}
	if tag.TimeLayout != "" {
		timeLayout = tag.TimeLayout
	}
	zeroValue := time.Time{}
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
