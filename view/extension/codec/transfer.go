package codec

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension/codec/jsontab"
	"github.com/viant/datly/view/extension/codec/transfer"
	"github.com/viant/datly/view/extension/codec/xmlfilter"
	"github.com/viant/datly/view/extension/codec/xmltab"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"reflect"
)

const (
	KeyTransfer = "Transfer"
)

type (
	TransferFactory struct{}

	entry struct {
		tag      *transfer.Tag
		selector *structology.Selector
	}

	Transfer struct {
		destType     *structology.StateType
		transfers    []*entry
		srcType      *structology.StateType
		srvXmlTab    *xmltab.Service
		srvXmlFilter *xmlfilter.Service
		srvJsonTab   *jsontab.Service
		filters      FiltersRegistry
	}
)

func (e *TransferFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if err := ValidateMinArgs(codecConfig, KeyTransfer, 1); err != nil {
		return nil, err
	}
	opts := NewOptions(codec.NewOptions(options))
	destTypeName := codecConfig.Args[0]
	if len(codecConfig.Args) > 1 {
		destTypeName = codecConfig.Args[1]
	}
	destType, err := types.LookupType(opts.LookupType, destTypeName)
	if err != nil {
		return nil, err
	}
	ret := &Transfer{
		srvXmlTab:    xmltab.New(),
		srvJsonTab:   jsontab.New(),
		srvXmlFilter: xmlfilter.New(),
	}
	return ret, ret.init(destType)
}

func (e *Transfer) init(destType reflect.Type) error {
	e.destType = structology.NewStateType(destType)
	transfers := e.destType.MatchByTag(transfer.TagName)
	for _, aTransfer := range transfers {
		tag := transfer.ParseTag(aTransfer.Tag().Get(transfer.TagName))
		if tag.From == "" {
			continue
		}
		e.transfers = append(e.transfers, &entry{selector: aTransfer, tag: tag})
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
	for _, aTransfer := range e.transfers {
		value, err := src.Value(aTransfer.tag.From)
		if err != nil {
			return nil, fmt.Errorf("failed to read: %s %w", aTransfer.tag.From, err)
		}

		switch aTransfer.tag.Codec { //TODO pass in ctx codec registry and generalize it
		case KeyFilters:
			aCodec, _ := e.filters.New(&codec.Config{})
			value, err = aCodec.Value(ctx, value)
			if err != nil {
				return nil, err
			}
		case KeyXmlTab:
			value, err = e.srvXmlTab.Transfer(value)
			if err != nil {
				return nil, err
			}
		case KeyJsonTab:
			value, err = e.srvJsonTab.Transfer(value)
			if err != nil {
				return nil, err
			}
		case KeyXmlFilter:
			value, err = e.srvXmlFilter.Transfer(value)
			if err != nil {
				return nil, err
			}
		}

		if value != nil {
			if err = aTransfer.selector.SetValue(dest.Pointer(), value); err != nil {
				return nil, fmt.Errorf("failed to set: %s %w", aTransfer.selector.Name(), err)
			}
		}
	}
	return dest.State(), nil
}
