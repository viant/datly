package transcribe

import "github.com/viant/datly/transcribe/internal/handlergen"

func (r *Result) handlerContext() *handlergen.Context {
	if r == nil {
		return nil
	}
	result := &handlergen.Context{
		Component: r.Component, TypeAuthority: r.TypeAuthority, TypeContext: r.TypeContext,
		ViewBindings: r.ViewBindings, ContractTypeOverrides: r.ContractTypeOverrides,
		GoHandler: r.GoHandler, VeltyHandler: r.VeltyHandler, ExternalHandler: r.ExternalHandler,
	}
	if r.Source != nil {
		result.Types = r.Source.Types
	}
	return result
}

func (r *Result) handlerDiagnostic(cause error) error {
	if cause == nil || r == nil || r.Source == nil {
		return cause
	}
	return &CompileError{Cause: cause, Diagnostics: []*Diagnostic{{Code: "DQL-HANDLER", Severity: SeverityError, Message: cause.Error(), Path: r.Source.Path, Span: pointSpan(r.Source.Text, 0)}}}
}
