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
	// CacheName is the resolved logical name or explicit view binding; it may be empty.
	CacheName string
	// Backend is afs, aerospike, or empty when the selected service is unknown.
	Backend string
	// Work describes population performed by this scheduled target, including
	// IndexMeta output views. These entries are not additional scheduled targets.
	Work []ReaderWarmupWork
}

// ReaderWarmupWork describes one actual view/policy selected by a target.
// Its originating scheduled policy is the containing ReaderWarmupTarget.
// CacheName is a logical name; neither it nor Backend contains storage addresses.
type ReaderWarmupWork struct {
	View      string
	Settings  *spec.CacheWarmupSettings
	CacheName string
	Backend   string
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

// ReaderWarmupTargeter exposes reader-owned warmup targets after initialization.
// Native readers return detached settings and descriptive work metadata; callers
// must schedule only the top-level targets, never their Work entries.
type ReaderWarmupTargeter interface {
	WarmupTargets() []ReaderWarmupTarget
}
