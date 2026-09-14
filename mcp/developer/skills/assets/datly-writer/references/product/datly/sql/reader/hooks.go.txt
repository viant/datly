package reader

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/data"
	rcollector "github.com/viant/datly/sql/reader/collector"
	"github.com/viant/datly/sql/reader/rowcodec"
	xhandler "github.com/viant/xdatly/handler"
)

type rowHookVisitor struct {
	ctx            context.Context
	view           *data.View
	collector      *rcollector.Collector
	downstream     rcollector.VisitorFn
	parentProvider func(any) (any, error)
	decoder        *rowcodec.Decoder
	evidence       *columnEvidence
}

func newRowHookVisitor(ctx context.Context, view *data.View, collector *rcollector.Collector, downstream rcollector.VisitorFn) *rowHookVisitor {
	return &rowHookVisitor{ctx: ctx, view: view, collector: collector, downstream: downstream}
}

func (v *rowHookVisitor) Visit(row any) error {
	_, err := v.VisitRow(row)
	return err
}

func (v *rowHookVisitor) VisitRow(row any) (any, error) {
	if v.decoder != nil {
		decoded, err := v.decoder.Decode(v.ctx, row)
		if err != nil {
			return nil, err
		}
		row = decoded
	}
	if v.collector != nil && v.evidence != nil {
		v.collector.RecordFields(v.evidence.fields)
	}
	if fetcher, ok := row.(xhandler.OnFetcher); ok {
		hookContext, err := v.hookContext(row)
		if err != nil {
			return nil, err
		}
		if err := fetcher.OnFetch(hookContext); err != nil {
			return nil, err
		}
	}
	if v.downstream != nil {
		if err := v.downstream(row); err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (v *rowHookVisitor) hookContext(row any) (context.Context, error) {
	if v.view == nil || !v.view.Spec.PublishParent || v.collector == nil || v.collector.Parent() == nil {
		return v.ctx, nil
	}
	if v.parentProvider == nil {
		v.parentProvider = v.collector.ParentRow()
	}
	if v.parentProvider == nil {
		return v.ctx, nil
	}
	parent, err := v.parentProvider(row)
	if err != nil {
		return nil, fmt.Errorf("resolve OnFetch parent row: %w", err)
	}
	if parent == nil {
		return v.ctx, nil
	}
	hookContext := context.WithValue(v.ctx, xhandler.DataSyncKey, v.collector.Parent().DataSync())
	return context.WithValue(hookContext, reflect.TypeOf(parent), parent), nil
}

func runOnRelation(ctx context.Context, row any) bool {
	if hook, ok := row.(xhandler.OnRelationer); ok {
		hook.OnRelation(ctx)
		return true
	}
	return false
}
