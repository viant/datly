package state

import (
	"context"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type (
	Resourcelet interface {
		LookupParameter(name string) (*Parameter, error)
		ViewSchema(ctx context.Context, schema string) (*Schema, error)
		LookupType() xreflect.LookupType
		LoadText(ctx context.Context, URL string) (string, error)
		NamedCodecs() *codec.Registry

		//CodecOptions returns base codec options
		CodecOptions() *codec.Options
	}
)