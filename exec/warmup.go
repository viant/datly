package exec

import (
	"context"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

// ReaderWarmupRequest selects one prepared view and its authored warmup policy.
// It is a trusted startup/operational request, not a transport parameter.
type ReaderWarmupRequest struct {
	View     string
	Settings *spec.CacheWarmupSettings
}

// ReaderWarmupTarget is one materialized view warmup policy owned by a reader.
type ReaderWarmupTarget struct {
	View     string
	Settings *spec.CacheWarmupSettings
}

// ReaderWarmupInvocation carries canonical input already bound by the engine.
type ReaderWarmupInvocation struct {
	Request    ReaderWarmupRequest
	Input      any
	Binder     xhandler.Binder
	Parameters sqlx.ParameterResolver
}

// ReaderWarmer is an optional reader operation; native SQLX owns all cache data.
type ReaderWarmer interface {
	Warmup(context.Context, ReaderWarmupInvocation) (int, error)
}

// ReaderWarmupTargeter exposes reader-owned warmup targets after compilation.
type ReaderWarmupTargeter interface {
	WarmupTargets() []ReaderWarmupTarget
}
