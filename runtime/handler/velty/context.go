package velty

import (
	"context"

	rhandler "github.com/viant/datly/runtime/handler"
	xbind "github.com/viant/xdatly/bind"
	xdiffer "github.com/viant/xdatly/differ"
	xhandler "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	xmbus "github.com/viant/xdatly/mbus"
)

// Context is the explicit Velty capability surface. The compiled program
// exposes typed input separately as $Input/$input and individual fields.
type Context struct {
	Differ     *Differ     `velty:"names=differ"`
	DML        *DML        `velty:"names=dml"`
	Index      *Index      `velty:"names=index"`
	Sequencer  *Sequencer  `velty:"names=sequencer"`
	Validator  *Validator  `velty:"names=validator"`
	MessageBus *MessageBus `velty:"names=messageBus"`
	Logger     *Logger     `velty:"names=logger"`
	WriteHooks *WriteHooks `velty:"names=writeHooks"`
}

func newContext(ctx context.Context, binder xhandler.Binder, capabilities programCapabilities) (*Context, error) {
	result := &Context{}
	if capabilities.differ {
		service, _, err := xbind.Lookup[xdiffer.Differ](ctx, binder, rhandler.DifferCapabilityKey)
		if err != nil {
			return nil, err
		}
		result.Differ = &Differ{ctx: ctx, service: service}
	}
	if capabilities.writeHooks {
		result.WriteHooks = &WriteHooks{ctx: ctx}
	}
	if capabilities.index {
		result.Index = newIndex()
	}
	if capabilities.dml {
		dml, _, err := xbind.Lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
		if err != nil {
			return nil, err
		}
		result.DML = &DML{data: dml}
	}
	if capabilities.sequencer {
		sequencer, _, err := xbind.Lookup[xhandler.Sequencer](ctx, binder, xhandler.SequencerKey)
		if err != nil {
			return nil, err
		}
		result.Sequencer = &Sequencer{ctx: ctx, service: sequencer}
	}
	if capabilities.validator {
		validator, _, err := xbind.Lookup[rhandler.ValidationService](ctx, binder, rhandler.ValidatorCapabilityKey)
		if err != nil {
			return nil, err
		}
		result.Validator = &Validator{ctx: ctx, service: validator}
	}
	if capabilities.messageBus {
		messageBus, _, err := xbind.Lookup[xmbus.Service](ctx, binder, rhandler.MessageBusCapabilityKey)
		if err != nil {
			return nil, err
		}
		result.MessageBus = &MessageBus{ctx: ctx, service: messageBus}
	}
	if capabilities.logger {
		log, _, err := xbind.Lookup[xlogger.Logger](ctx, binder, rhandler.LoggerCapabilityKey)
		if err != nil {
			return nil, err
		}
		result.Logger = &Logger{service: log}
	}
	return result, nil
}
