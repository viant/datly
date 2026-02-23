package compile

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/dml"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
	"github.com/viant/datly/repository/shape/plan"
)

// DQLCompiler compiles raw DQL into a shape plan that can be materialized by shape/load.
type DQLCompiler struct{}

// New returns a DQL compiler implementation.
func New() *DQLCompiler {
	return &DQLCompiler{}
}

// CompileError represents one or more compilation diagnostics.
type CompileError struct {
	Diagnostics []*dqlshape.Diagnostic
}

func (e *CompileError) Error() string {
	if e == nil || len(e.Diagnostics) == 0 {
		return "shape compile failed"
	}
	first := e.Diagnostics[0]
	if len(e.Diagnostics) == 1 {
		return first.Error()
	}
	return fmt.Sprintf("%s (and %d more diagnostics)", first.Error(), len(e.Diagnostics)-1)
}

// Compile implements shape.DQLCompiler.
func (c *DQLCompiler) Compile(_ context.Context, source *shape.Source, opts ...shape.CompileOption) (*shape.PlanResult, error) {
	if source == nil {
		return nil, shape.ErrNilSource
	}
	compileOptions := applyCompileOptions(opts)
	pathLayout := newCompilePathLayout(compileOptions)
	compileProfile := normalizeCompileProfile(compileOptions.Profile)
	enforceStrict := compileOptions.Strict || compileProfile == shape.CompileProfileStrict
	if strings.TrimSpace(source.DQL) == "" {
		return nil, shape.ErrNilDQL
	}

	pre := dqlpre.Prepare(source.DQL)
	pre.TypeCtx = applyTypeContextDefaults(pre.TypeCtx, source, compileOptions, pathLayout)
	pre.Diagnostics = append(pre.Diagnostics, typeContextDiagnostics(pre.TypeCtx, enforceStrict)...)
	allDiags := append([]*dqlshape.Diagnostic{}, pre.Diagnostics...)
	if hasErrorDiagnostics(allDiags) {
		return nil, &CompileError{Diagnostics: allDiags}
	}

	statements := dqlstmt.New(pre.SQL)
	decision := pipeline.Classify(statements)
	prepared := buildHandlerIfNeeded(source, pre, statements, decision, pathLayout)
	pre = prepared.Pre
	statements = prepared.Statements
	decision = prepared.Decision
	legacyFallbackViews := prepared.LegacyViews
	effectiveSource := source
	if prepared.EffectiveSource != nil {
		effectiveSource = prepared.EffectiveSource
	}
	if strings.TrimSpace(pre.SQL) == "" && len(legacyFallbackViews) == 0 {
		allDiags = append(allDiags, &dqlshape.Diagnostic{
			Code:     dqldiag.CodeParseEmpty,
			Severity: dqlshape.SeverityError,
			Message:  "no SQL statement found",
			Hint:     "add SELECT/INSERT/UPDATE/DELETE statement after DQL directives",
			Span: dqlshape.Span{
				Start: dqlshape.Position{Line: 1, Char: 1},
				End:   dqlshape.Position{Line: 1, Char: 1},
			},
		})
		return nil, &CompileError{Diagnostics: allDiags}
	}
	var root *plan.View
	var compileDiags []*dqlshape.Diagnostic
	var err error
	if len(legacyFallbackViews) > 0 {
		root = legacyFallbackViews[0]
	} else {
		root, compileDiags, err = c.compileRoot(source.Name, pre.SQL, statements, decision, compileOptions.MixedMode, compileOptions.UnknownNonReadMode)
	}
	if err != nil {
		return nil, err
	}
	pre.Mapper.Remap(compileDiags)
	allDiags = append(allDiags, compileDiags...)
	if root == nil {
		return nil, &CompileError{Diagnostics: allDiags}
	}

	result := newPlanResult(root)
	if len(legacyFallbackViews) > 1 {
		for _, item := range legacyFallbackViews[1:] {
			if item == nil || strings.TrimSpace(item.Name) == "" {
				continue
			}
			if _, exists := result.ViewsByName[item.Name]; exists {
				continue
			}
			result.Views = append(result.Views, item)
			result.ViewsByName[item.Name] = item
		}
	}
	result.Diagnostics = allDiags
	result.TypeContext = pre.TypeCtx
	result.Directives = pre.Directives
	applyDefaultConnectorDirective(result)
	hints := extractViewHints(source.DQL)
	appendRelationViews(result, root, hints)
	appendDeclaredViews(source.DQL, result)
	appendDeclaredStates(source.DQL, result)
	if prepared.ForceLegacyContract && len(legacyFallbackViews) > 0 {
		if legacyStates := resolveLegacyRouteStatesWithLayout(effectiveSource, pathLayout); len(legacyStates) > 0 {
			result.States = legacyStates
		}
		if legacyTypes := resolveLegacyRouteTypesWithLayout(effectiveSource, pathLayout); len(legacyTypes) > 0 {
			result.Types = legacyTypes
		}
	}
	result.Diagnostics = append(result.Diagnostics, appendComponentTypesWithLayout(effectiveSource, result, pathLayout)...)
	mergeLegacyRouteStatesWithLayout(result, effectiveSource, pathLayout)
	mergeLegacyRouteTypesWithLayout(result, effectiveSource, pathLayout)
	applyViewHints(result, hints)
	applySourceParityEnrichmentWithLayout(result, effectiveSource, pathLayout)
	result.Diagnostics = append(result.Diagnostics, applyColumnDiscoveryPolicy(result, compileOptions)...)
	if len(result.States) == 0 && len(legacyFallbackViews) > 0 {
		result.States = resolveLegacyRouteStatesWithLayout(effectiveSource, pathLayout)
	}
	if len(result.Types) == 0 && len(legacyFallbackViews) > 0 {
		result.Types = resolveLegacyRouteTypesWithLayout(effectiveSource, pathLayout)
	}

	if enforceStrict && hasEscalationWarnings(result.Diagnostics) {
		return nil, &CompileError{Diagnostics: filterEscalationDiagnostics(result.Diagnostics)}
	}
	if hasErrorDiagnostics(result.Diagnostics) {
		return nil, &CompileError{Diagnostics: result.Diagnostics}
	}
	return &shape.PlanResult{Source: source, Plan: result}, nil
}

func applyDefaultConnectorDirective(result *plan.Result) {
	if result == nil || result.Directives == nil {
		return
	}
	connector := strings.TrimSpace(result.Directives.DefaultConnector)
	if connector == "" {
		return
	}
	for _, item := range result.Views {
		if item == nil || strings.TrimSpace(item.Connector) != "" {
			continue
		}
		item.Connector = connector
	}
}

func (c *DQLCompiler) compileRoot(sourceName, sqlText string, statements dqlstmt.Statements, decision pipeline.Decision, mode shape.CompileMixedMode, unknownMode shape.CompileUnknownNonReadMode) (*plan.View, []*dqlshape.Diagnostic, error) {
	mode = normalizeMixedMode(mode)
	unknownMode = normalizeUnknownNonReadMode(unknownMode)
	if !decision.HasRead && !decision.HasExec && decision.HasUnknown {
		diag := &dqlshape.Diagnostic{
			Code:     dqldiag.CodeParseUnknownNonRead,
			Severity: dqlshape.SeverityWarning,
			Message:  "no readable SELECT statement detected",
			Hint:     "use SELECT for read parsing or compile as DML/handler template",
			Span:     pipeline.StatementSpan(sqlText, statements[0]),
		}
		if unknownMode == shape.CompileUnknownNonReadError {
			diag.Severity = dqlshape.SeverityError
			return nil, []*dqlshape.Diagnostic{diag}, nil
		}
		view, execDiags := pipeline.BuildExec(sourceName, sqlText, statements)
		return view, append([]*dqlshape.Diagnostic{diag}, execDiags...), nil
	}
	if decision.HasRead && decision.HasExec {
		switch mode {
		case shape.CompileMixedModeErrorOnMixed:
			return nil, []*dqlshape.Diagnostic{
				{
					Code:     dqldiag.CodeDMLMixed,
					Severity: dqlshape.SeverityError,
					Message:  "mixed read/exec script is not allowed by compile mixed mode",
					Hint:     "use WithMixedMode(shape.CompileMixedModeExecWins) or split handlers",
					Span:     pipeline.StatementSpan(sqlText, statements[0]),
				},
			}, nil
		case shape.CompileMixedModeReadWins:
			readSQL := sqlText
			for _, stmt := range statements {
				if stmt != nil && stmt.Kind == dqlstmt.KindRead {
					readSQL = sqlText[stmt.Start:stmt.End]
					break
				}
			}
			view, diags, err := pipeline.BuildRead(sourceName, readSQL)
			diags = append(diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeDMLMixed,
				Severity: dqlshape.SeverityWarning,
				Message:  "mixed read/exec script detected; read compilation path selected",
				Hint:     "split SELECT and DML into separate handlers when possible",
				Span:     pipeline.StatementSpan(sqlText, statements[0]),
			})
			return view, diags, err
		}
	}
	if decision.HasExec {
		view, diags := dml.Compile(sourceName, sqlText, statements)
		if decision.HasRead {
			diags = append(diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeDMLMixed,
				Severity: dqlshape.SeverityWarning,
				Message:  "mixed read/exec script detected; exec compilation path selected",
				Hint:     "split SELECT and DML into separate handlers when possible",
				Span:     pipeline.StatementSpan(sqlText, statements[0]),
			})
		}
		return view, diags, nil
	}
	return pipeline.BuildRead(sourceName, sqlText)
}

func normalizeMixedMode(mode shape.CompileMixedMode) shape.CompileMixedMode {
	switch mode {
	case shape.CompileMixedModeExecWins, shape.CompileMixedModeReadWins, shape.CompileMixedModeErrorOnMixed:
		return mode
	default:
		return shape.CompileMixedModeExecWins
	}
}

func normalizeUnknownNonReadMode(mode shape.CompileUnknownNonReadMode) shape.CompileUnknownNonReadMode {
	switch mode {
	case shape.CompileUnknownNonReadWarn, shape.CompileUnknownNonReadError:
		return mode
	default:
		return shape.CompileUnknownNonReadWarn
	}
}

func normalizeCompileProfile(profile shape.CompileProfile) shape.CompileProfile {
	switch profile {
	case shape.CompileProfileCompat, shape.CompileProfileStrict:
		return profile
	default:
		return shape.CompileProfileCompat
	}
}

func newPlanResult(root *plan.View) *plan.Result {
	result := &plan.Result{
		Views:       []*plan.View{root},
		ViewsByName: map[string]*plan.View{},
		ByPath:      map[string]*plan.Field{},
	}
	result.ViewsByName[root.Name] = root
	return result
}

func applyCompileOptions(opts []shape.CompileOption) *shape.CompileOptions {
	ret := &shape.CompileOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(ret)
		}
	}
	return ret
}
