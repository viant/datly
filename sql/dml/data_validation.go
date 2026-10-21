package dml

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

// FrameworkValidator is an optional internal Data capability. Engine adapters
// expose only Validator, retaining database and transaction ownership here.
func (d *Data) FrameworkValidator() xhandler.Validator {
	if d == nil {
		return nil
	}
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.frameworkValidator == nil {
		owner.frameworkValidator = validation.New(owner)
	}
	return dataValidation{data: d, service: owner.frameworkValidator}
}

type dataValidation struct {
	data    *Data
	service *validation.Service
}

func (v dataValidation) Validate(ctx context.Context, value any, options ...any) (*xhandler.Validation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("framework validation context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner := v.data.owner()
	effect, err := drainowner.BeginNativeEffect(owner)
	if err != nil {
		return nil, err
	}
	defer drainowner.EndDrain(effect)
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	open := v.data.open
	owner.mu.Unlock()
	if !open {
		return nil, ErrComponentSealed
	}
	// Resolve immutable owner identity before bufferedParents takes owner.mu.
	// Distinct typed-delete evidence must not infer a missing database dialect.
	if owner.db != nil {
		if _, err := owner.dialectFor(ctx, owner.db); err != nil {
			return nil, err
		}
	}
	parents := v.data.bufferedParents()
	if len(parents) != 0 {
		prepared, checks, err := v.service.PrepareBufferedValidation(ctx, value, options, parents)
		if err != nil {
			return nil, err
		}
		for _, check := range checks {
			owner.mu.Lock()
			err = owner.registerExecutionGuardLocked(check, drainowner.NewGuardBinding(), true)
			owner.mu.Unlock()
			if err != nil {
				return nil, err
			}
		}
		options = prepared
	}
	return v.service.Validate(ctx, value, options...)
}

// MatchReference is a protected native validation capability for generated
// writers and trusted application-owned buffered writers. It preserves the
// same component lifetime and serialized transaction access as Validate.
// Matching keys alone does not prove an earlier successful parent INSERT.
func (v dataValidation) MatchReference(ctx context.Context, child, parent any, expected xhandler.ValidationReference) (*xhandler.ValidationReference, error) {
	owner := v.data.owner()
	effect, err := drainowner.BeginNativeEffect(owner)
	if err != nil {
		return nil, err
	}
	defer drainowner.EndDrain(effect)
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	open := v.data.open
	owner.mu.Unlock()
	if !open {
		return nil, ErrComponentSealed
	}
	return v.service.MatchReference(ctx, child, parent, expected)
}

func (d *Data) ValidationConnection(ctx context.Context, connector string) (validation.Connection, error) {
	if err := ctx.Err(); err != nil {
		return validation.Connection{}, err
	}
	// The Data unit is already bound to a specific source. A named selection
	// needs explicit registration; never reinterpret an unknown name as this DB.
	if connector != "" {
		return validation.Connection{}, fmt.Errorf("validation connector %q is not registered in this data unit", connector)
	}
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.completed {
		return validation.Connection{}, ErrInvocationCompleted
	}
	if owner.failed != nil {
		return validation.Connection{}, fmt.Errorf("%w: %v", ErrInvocationFailed, owner.failed)
	}
	if owner.db == nil {
		return validation.Connection{}, fmt.Errorf("validation database is required")
	}
	return validation.Connection{DB: owner.db, Tx: owner.tx, Dialect: owner.dialect}, nil
}
