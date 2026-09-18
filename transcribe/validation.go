package transcribe

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/constant"
	"path/filepath"

	"github.com/viant/datly/transcribe/column"
)

// Validator is the non-persisting authoring validation entry point for CLI and
// developer tooling. It reuses discovery and generation planning; it never
// registers components or executes application code.
type Validator struct {
	Const      *constant.Values
	BaseDir    string
	ModuleDirs []string
	Include    []string
	Exclude    []string
	// ColumnRefiner explicitly enables database discovery through the existing
	// compiler owner. Nil keeps validation static. Connections remain caller-owned.
	ColumnRefiner *column.Refiner
	Connector     string
}

type ValidationReport struct {
	Valid       bool               `json:"valid"`
	Components  []string           `json:"components"`
	Completed   []string           `json:"completed"`
	Skipped     []string           `json:"skipped"`
	Diagnostics []*Diagnostic      `json:"diagnostics,omitempty"`
	Schema      []SchemaInspection `json:"schema,omitempty"`
}

// Validate checks the selected source and prospective generated artifacts in
// memory. Valid means the reported stages passed, not runtime readiness.
func (v *Validator) Validate(ctx context.Context) (*ValidationReport, error) {
	report := &ValidationReport{Components: []string{}, Completed: []string{},
		Skipped: []string{"database schema and constraints", "Go compilation and runtime service resolution", "runtime service and hook execution", "SQLite behavior fixtures", "HTTP/MCP runtime exposure"}}
	if v == nil {
		return report, report.failure(fmt.Errorf("validator is required"))
	}
	if v.ColumnRefiner != nil {
		report.Skipped[0] = "runtime payload validation and database constraints beyond discovered column metadata"
	}
	if err := ctx.Err(); err != nil {
		return report, report.failure(err)
	}
	directory, err := v.Const.Path(v.BaseDir)
	if err != nil {
		return report, report.failure(err)
	}
	base, err := filepath.Abs(directory)
	if err != nil {
		return report, report.failure(err)
	}
	moduleDirs := append([]string(nil), v.ModuleDirs...)
	for i, path := range moduleDirs {
		moduleDirs[i], err = v.Const.Path(path)
		if err != nil {
			return report, report.failure(err)
		}
	}
	project, err := (&Discovery{BaseDir: base, ModuleDirs: moduleDirs, Include: v.Include, Exclude: v.Exclude,
		Const: v.Const, Connector: v.Connector, ColumnRefiner: v.ColumnRefiner}).Compile(ctx)
	if err != nil {
		return report, report.failure(err)
	}
	if len(project.Components) == 0 {
		return report, report.failure(fmt.Errorf("no components found in selected packages"))
	}
	report.Completed = append(report.Completed, "source discovery and contract compilation")
	if v.ColumnRefiner != nil {
		if err := report.schemaDiscovery(project); err != nil {
			return report, report.failure(err)
		}
	}
	if err = ctx.Err(); err != nil {
		return report, report.failure(err)
	}
	prepared, err := project.prepareEphemeral(base)
	if err != nil {
		return report, report.failure(err)
	}
	if err = ctx.Err(); err != nil {
		return report, report.failure(err)
	}
	for _, component := range prepared {
		report.Components = append(report.Components, component.identity)
	}
	report.Completed = append(report.Completed, "generation plan, resources, component dependencies and route conflicts", "generated destination layout and imports (no writes)")
	report.Valid = true
	return report, nil
}

func (r *ValidationReport) failure(err error) error {
	var compiled *CompileError
	if errors.As(err, &compiled) && len(compiled.Diagnostics) != 0 {
		r.Diagnostics = append(r.Diagnostics, compiled.Diagnostics...)
	} else {
		r.Diagnostics = append(r.Diagnostics, &Diagnostic{Code: "VALIDATE", Severity: SeverityError, Message: err.Error()})
	}
	return err
}
