package transcribe

import (
	"errors"
	"fmt"

	"github.com/viant/datly/spec"
	tcompile "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	"github.com/viant/datly/typecatalog"
)

type readPlanCompiler struct {
	sourceMap *SourceMap
	path      string
	types     *typecatalog.Resolver
}

const (
	codeReadStatementSelection = "DQL-READ-STATEMENT-SELECTION"
	codeReadTemplateFrame      = "DQL-READ-TEMPLATE-FRAME"
)

func (c *readPlanCompiler) compile(component *spec.Component, prepared *dql.PreparedSource) (*spec.View, error) {
	if component == nil || component.RootView == nil || prepared == nil {
		return nil, nil
	}
	var reads []*statement.Statement
	for _, item := range prepared.Statements {
		if item != nil && item.Kind == statement.KindRead {
			reads = append(reads, item)
		}
	}
	if len(reads) == 0 {
		return nil, nil
	}
	if len(prepared.Statements) != 1 || len(reads) != 1 {
		offset := reads[0].SQLStart
		position := c.sourceMap.Position(offset)
		diagnostic := &Diagnostic{
			Code: codeReadStatementSelection, Severity: SeverityError,
			Message: "read compilation requires exactly one authored statement", Path: c.path,
			Span: Span{Start: position, End: c.sourceMap.Position(nextOffset(prepared.SQL, offset))},
		}
		return nil, &CompileError{Cause: diagnostic, Diagnostics: []*Diagnostic{diagnostic}}
	}
	item := reads[0]
	if !item.TemplateBalanced {
		diagnostic := &Diagnostic{
			Code: codeReadTemplateFrame, Severity: SeverityError,
			Message: "read template block opened before SQL is not balanced", Path: c.path,
			Span: Span{Start: c.sourceMap.Position(item.Start), End: c.sourceMap.Position(item.SQLStart)},
		}
		return nil, &CompileError{Cause: diagnostic, Diagnostics: []*Diagnostic{diagnostic}}
	}
	if item.SQLStart < item.Start || item.SQLStart >= item.SQLEnd || item.SQLEnd > item.End || item.End > len(prepared.SQL) {
		return nil, fmt.Errorf("invalid read statement SQL span [%d:%d]", item.SQLStart, item.SQLEnd)
	}
	frame := tcompile.TemplateFrame{}
	if item.Start < item.SQLStart {
		frame.Prefix = prepared.SQL[item.Start:item.SQLStart]
	}
	if item.SQLEnd < item.End {
		frame.Suffix = prepared.SQL[item.SQLEnd:item.End]
	}
	result, err := tcompile.NewReader().Compile(tcompile.ReadInput{
		View: component.RootView, SQL: prepared.SQL[item.SQLStart:item.SQLEnd], Template: frame, Types: c.types, TypeContext: component.TypeContext,
	})
	if err == nil {
		if component.Settings != nil {
			if err = resolveConstantViewTables(result, component.Settings.Const); err != nil {
				span := pointSpan(prepared.Original, 0)
				if authored, ok := constantDiagnosticSpan(prepared, err); ok {
					span = Span{Start: positionAt(prepared.Original, authored.Start), End: positionAt(prepared.Original, authored.End)}
				}
				diagnostic := &Diagnostic{Code: "DQL-CONST", Severity: SeverityError, Message: err.Error(), Path: c.path, Span: span}
				return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
			}
		}
		return result, nil
	}
	offset := item.SQLStart
	end := offset
	code := tcompile.CodeSQLParse
	var compileError *tcompile.Error
	if errors.As(err, &compileError) {
		code = compileError.Code
		offset += compileError.Offset
		if compileError.End > compileError.Offset {
			end = item.SQLStart + compileError.End
		}
	}
	if end <= offset || end > item.End {
		end = nextOffset(prepared.SQL, offset)
	}
	position := c.sourceMap.Position(offset)
	diagnostic := &Diagnostic{
		Code: code, Severity: SeverityError, Message: err.Error(), Path: c.path,
		Span: Span{Start: position, End: c.sourceMap.Position(end)},
	}
	return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
}
