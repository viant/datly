package codec

import (
	"context"
	"github.com/viant/datly/utils/types"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strings"
)

const (
	KeyTransfer = "Transfer"
	TransferTag = "transfer"
)

type (
	TransferFactory struct{}

	entry struct {
		from     string
		selector *structology.Selector
	}
	Transfer struct {
		destType  *structology.StateType
		transfers []*entry
		srcType   *structology.StateType
	}
)

func (e *TransferFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if err := ValidateMinArgs(codecConfig, KeyTransfer, 1); err != nil {
		return nil, err
	}
	opts := NewOptions(codec.NewOptions(options))
	destType, err := types.LookupType(opts.LookupType, codecConfig.Args[0])
	if err != nil {
		return nil, err
	}
	ret := &Transfer{}
	return ret, ret.init(destType)
}

func (e *Transfer) init(destType reflect.Type) error {
	e.destType = structology.NewStateType(destType)
	transfers := e.destType.MatchByTag(TransferTag)
	for _, transfer := range transfers {
		tag, _ := transfer.Tag().Lookup(TransferTag)
		from := ""
		if strings.HasPrefix(tag, "from=") {
			from = tag[5:]
		}
		e.transfers = append(e.transfers, &entry{selector: transfer, from: from})
	}
	return nil
}

func (e *Transfer) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return e.destType.Type(), nil
}

func (e *Transfer) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	if e.srcType == nil {
		e.srcType = structology.NewStateType(reflect.TypeOf(raw))
	}
	src := e.srcType.WithValue(raw)
	dest := e.destType.NewState()
	for _, transfer := range e.transfers {
		value, err := src.Value(transfer.from)
		if err != nil {
			return nil, err
		}
		if value != nil {
			if err = transfer.selector.SetValue(dest.Pointer(), value); err != nil {
				return nil, err
			}
		}
	}
	return dest.State(), nil
}
