package write

import (
	"context"
	"errors"
	"testing"
	"time"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type handlerTestData struct {
	nextID  int
	inserts []*Record
	updates []*Record
	flushes int
}

func (d *handlerTestData) Insert(table string, value any) error {
	if table != recordTable {
		return errors.New("unexpected insert table")
	}
	d.inserts = append(d.inserts, value.(*Record))
	return nil
}

func (d *handlerTestData) Update(table string, value any) error {
	if table != recordTable {
		return errors.New("unexpected update table")
	}
	d.updates = append(d.updates, value.(*Record))
	return nil
}

func (*handlerTestData) Delete(string, any) error {
	return errors.New("unexpected delete")
}

func (*handlerTestData) Execute(string, ...any) error {
	return errors.New("unexpected execute")
}

func (d *handlerTestData) Allocate(_ context.Context, table string, dest any, selector string) error {
	if table != recordTable || selector != "ID" {
		return errors.New("unexpected allocation")
	}
	for _, record := range dest.([]*Record) {
		if record != nil && record.ID == 0 {
			d.nextID++
			record.ID = d.nextID
		}
	}
	return nil
}

func (d *handlerTestData) Flush(context.Context, string) error {
	d.flushes++
	return nil
}

type handlerTestBinder struct {
	data xhandler.Data
}

func (*handlerTestBinder) Bind(context.Context, any) error {
	return nil
}

func (b *handlerTestBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	switch key {
	case xhandler.DataKey:
		return b.data, true, nil
	}
	return nil, false, nil
}

type handlerTestLogger struct {
	debug int
	info  int
	warn  int
}

func (l *handlerTestLogger) Debug(string, ...any) { l.debug++ }
func (l *handlerTestLogger) Info(string, ...any)  { l.info++ }
func (l *handlerTestLogger) Warn(string, ...any)  { l.warn++ }
func (*handlerTestLogger) Error(string, ...any)   {}

type handlerTestValidator struct {
	calls      int
	violations []string
}

func (v *handlerTestValidator) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	v.calls++
	result := &xhandler.Validation{}
	for _, message := range v.violations {
		result.Violations = append(result.Violations, &xhandler.Violation{Message: message})
	}
	return result, nil
}

type handlerTestSession struct {
	binder xhandler.Binder
}

func (s *handlerTestSession) Binder() xhandler.Binder {
	return s.binder
}

func (*handlerTestSession) Response() xresponse.Writer {
	return nil
}

func TestRecordPatchHandlerStagesInsertAndUpdateWithoutFlushing(t *testing.T) {
	requestTime := time.Date(2026, time.July, 27, 10, 30, 0, 0, time.UTC)
	data := &handlerTestData{nextID: 100}
	log := &handlerTestLogger{}
	validator := &handlerTestValidator{}
	session := &handlerTestSession{binder: &handlerTestBinder{data: data}}
	input := &RecordInput{
		Auth:           &AuthOutput{Allowed: true},
		RequestTime:    requestTime,
		CurrentRecords: []*Record{{ID: 7}},
		Records: []*Record{
			{ID: 7, Name: "updated", Has: &RecordHas{Name: true}},
			{Name: "inserted", Has: &RecordHas{Name: true}},
		},
	}
	output := &RecordOutput{}

	handler := NewRecordPatchRuntimeHandler(xhandler.Capabilities{Logger: log, Validator: validator})
	actual, err := handler.Execute(context.Background(), rhandler.Invocation{
		Input:  input,
		Binder: session.Binder(),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	output = actual.(*RecordOutput)
	if len(data.updates) != 1 || data.updates[0].ID != 7 {
		t.Fatalf("updates = %+v", data.updates)
	}
	if len(data.inserts) != 1 || data.inserts[0].ID != 101 {
		t.Fatalf("inserts = %+v", data.inserts)
	}
	if data.flushes != 0 {
		t.Fatalf("child handler flushed root-owned data %d times", data.flushes)
	}
	if validator.calls != 1 || log.debug != 1 || log.info != 1 || log.warn != 0 {
		t.Fatalf("injected capabilities: validator=%d logger=%+v", validator.calls, log)
	}
	if output.Status != "ok" || len(output.Data) != 2 {
		t.Fatalf("output = %+v", output)
	}
	for _, record := range output.Data {
		if record.Updated == nil || !record.Updated.Equal(requestTime) || !record.Has.Updated {
			t.Fatalf("updated audit fields were not staged: %+v", record)
		}
	}
	if data.inserts[0].Created == nil || !data.inserts[0].Created.Equal(requestTime) || !data.inserts[0].Has.Created {
		t.Fatalf("insert audit fields were not staged: %+v", data.inserts[0])
	}
	if data.updates[0].Created != nil || data.updates[0].Has.Created {
		t.Fatalf("update unexpectedly changed created audit fields: %+v", data.updates[0])
	}
}

func TestRecordPatchHandlerStopsOnInjectedValidationViolations(t *testing.T) {
	data := &handlerTestData{}
	log := &handlerTestLogger{}
	validator := &handlerTestValidator{violations: []string{"name is required"}}
	binder := &handlerTestBinder{data: data}
	actual, err := NewRecordPatchRuntimeHandler(xhandler.Capabilities{Logger: log, Validator: validator}).Execute(context.Background(), rhandler.Invocation{
		Input: &RecordInput{
			Auth:    &AuthOutput{Allowed: true},
			Records: []*Record{{}},
		},
		Binder: binder,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	output := actual.(*RecordOutput)
	if output.Status != "error" || len(output.Violations) != 1 {
		t.Fatalf("output = %+v", output)
	}
	if len(data.inserts) != 0 || len(data.updates) != 0 || data.flushes != 0 {
		t.Fatalf("validation failure staged DML: inserts=%d updates=%d flushes=%d", len(data.inserts), len(data.updates), data.flushes)
	}
	if validator.calls != 1 || log.warn != 1 || log.info != 0 {
		t.Fatalf("injected capabilities: validator=%d logger=%+v", validator.calls, log)
	}
}

func TestRecordPatchHandlerRequiresAuthorization(t *testing.T) {
	_, err := NewRecordPatchRuntimeHandler(xhandler.Capabilities{}).Execute(context.Background(), rhandler.Invocation{
		Input:  &RecordInput{},
		Binder: (&handlerTestSession{}).Binder(),
	})
	if !errors.Is(err, ErrRecordUnauthorized) {
		t.Fatalf("Exec() error = %v, want %v", err, ErrRecordUnauthorized)
	}
}
