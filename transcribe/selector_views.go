package transcribe

import (
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
)

// resolveQuerySelectorViews canonicalizes authored SQL aliases while the
// transcribed view graph still carries them, so generated contracts bind
// selectors by canonical view name. Transcription sees only the DQL graph:
// nested views contributed by a linked Go output type are assembled later by
// artifact bootstrap, so a target unknown here is deferred, not rejected. The
// artifact build resolves every selector against the completed graph and
// rejects unknown or ambiguous targets before any execution.
func resolveQuerySelectorViews(component *spec.Component) error {
	return bootstrap.ResolveQuerySelectorViews(component, false)
}
