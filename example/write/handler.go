package write

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhandler "github.com/viant/datly/runtime/handler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	xhandler "github.com/viant/xdatly/handler"
)

const recordTable = "RECORDS"

var (
	ErrRecordInputRequired = errors.New("record patch input is required")
	ErrRecordUnauthorized  = errors.New("record patch is not authorized")
)

// RecordPatchHandler owns RECORDS write orchestration. Stable handler
// services are construction-injected fields. Request/transaction-scoped Data
// remains invocation-injected through the session binder.
type RecordPatchHandler struct {
	Logger    xhandler.Logger
	Validator xhandler.Validator
	// MessageBus is fixed-injected for post-commit coordination only. Exec does
	// not publish before the root transaction commits.
	MessageBus xhandler.MessageBus
}

var _ xhandler.Contract[RecordInput, RecordOutput] = (*RecordPatchHandler)(nil)

// NewRecordPatchHandler is the zero-argument authored factory retained by
// component metadata and transcription. Runtime assembly injects fixed
// primitives through NewRecordPatchRuntimeHandler.
func NewRecordPatchHandler() xhandler.Contract[RecordInput, RecordOutput] {
	return &RecordPatchHandler{}
}

// NewRecordPatchRuntimeHandler is the fixed runtime injection seam for the
// named component handler. Generated/bootstrap registration assigns its result
// to registry.RegisteredComponent.Handler.
func NewRecordPatchRuntimeHandler(capabilities xhandler.Capabilities) rhandler.TypedHandler {
	contract := &RecordPatchHandler{
		Logger:     capabilities.Logger,
		Validator:  capabilities.Validator,
		MessageBus: capabilities.MessageBus,
	}
	return customhandler.New[RecordInput, RecordOutput](contract)
}

func (h *RecordPatchHandler) Exec(ctx context.Context, session xhandler.Session, input *RecordInput, output *RecordOutput) error {
	if input == nil {
		return ErrRecordInputRequired
	}
	if input.Auth == nil || !input.Auth.Allowed {
		return ErrRecordUnauthorized
	}
	if session == nil || session.Binder() == nil {
		return errors.New("record patch session binder is required")
	}
	if output == nil {
		return errors.New("record patch output is required")
	}

	if h.Logger != nil {
		h.Logger.Debug("validating record patch", "records", len(input.Records))
	}

	if h.Validator != nil {
		result, validateErr := h.Validator.Validate(ctx, input.Records)
		if validateErr != nil {
			return fmt.Errorf("validate records: %w", validateErr)
		}
		if result.Err() != nil {
			violations := result.Messages()
			output.Status = "error"
			output.Violations = append(output.Violations, violations...)
			if h.Logger != nil {
				h.Logger.Warn("record patch validation failed", "violations", len(violations))
			}
			return nil
		}
	}

	value, found, err := session.Binder().Lookup(ctx, xhandler.DataKey)
	if err != nil {
		return fmt.Errorf("resolve record data capability: %w", err)
	}
	if !found {
		return errors.New("record data capability is unavailable")
	}
	data, ok := value.(xhandler.Data)
	if !ok || data == nil {
		return fmt.Errorf("record data capability has type %T, want handler.Data", value)
	}

	current := make(map[int]struct{}, len(input.CurrentRecords))
	for _, record := range input.CurrentRecords {
		if record != nil {
			current[record.ID] = struct{}{}
		}
	}

	// Allocate only prepares identifiers. It does not flush or complete the
	// root-owned transaction.
	if err = data.Allocate(ctx, recordTable, input.Records, "ID"); err != nil {
		return fmt.Errorf("allocate record identifiers: %w", err)
	}

	requestTime := input.RequestTime
	if requestTime.IsZero() {
		requestTime = time.Now().UTC()
	}
	for _, record := range input.Records {
		if record == nil {
			continue
		}
		if record.Has == nil {
			record.Has = &RecordHas{}
		}
		record.Updated = timePointer(requestTime)
		record.Has.Updated = true

		if _, exists := current[record.ID]; exists {
			if err = data.Update(recordTable, record); err != nil {
				return fmt.Errorf("update record %d: %w", record.ID, err)
			}
			continue
		}

		record.Created = timePointer(requestTime)
		record.Has.Created = true
		if err = data.Insert(recordTable, record); err != nil {
			return fmt.Errorf("insert record %d: %w", record.ID, err)
		}
	}

	output.Status = "ok"
	output.Data = input.Records
	if h.Logger != nil {
		h.Logger.Info("record patch staged", "records", len(output.Data))
	}
	return nil
}

func timePointer(value time.Time) *time.Time {
	return &value
}
