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

type timePtrMarshaller struct {
	timeLayout string
	zeroValue  string
	tag        *format.Tag
}

func newTimePtrMarshaller(tag *format.Tag, config *config.IOConfig) *timePtrMarshaller {
	timeLayout := time.RFC3339
	if layout := config.GetTimeLayout(); layout != "" {
		timeLayout = layout
	}
	if tag.TimeLayout != "" {
		timeLayout = tag.TimeLayout
	}

	zeroValue := null
	if !tag.IsNullable() {
		zeroValue = strconv.Quote(time.Time{}.Format(timeLayout))
	}
	return &timePtrMarshaller{
		timeLayout: timeLayout,
		zeroValue:  zeroValue,
		tag:        tag,
	}
}

func (t *timePtrMarshaller) UnmarshallObject(pointer unsafe.Pointer, decoder *gojay.Decoder, auxiliaryDecoder *gojay.Decoder, session *UnmarshalSession) error {
	aTime := xunsafe.AsTimeAddrPtr(pointer)

	timeDst := time.Time{}
	if err := decoder.AddTime(&timeDst, t.timeLayout); err != nil {
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

	return appendTime(sb, **aTime, t.timeLayout)
}
