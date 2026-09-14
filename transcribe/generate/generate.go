package generate

import "github.com/viant/datly/spec"

type Result struct {
	Plan        *Plan
	Files       []EmittedFile
	Diagnostics []spec.Diagnostic
}
