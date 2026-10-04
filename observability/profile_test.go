package observability

import (
	internallog "github.com/viant/datly/internal/logging"
	"reflect"
	"testing"
)

func TestLoggingPolicyCopy(t *testing.T) {
	var absent *Logging
	if absent.Copy() != nil {
		t.Fatal("nil profile changed")
	}
	yes, no := true, false
	original := &Logging{EnableAudit: &yes, EnableTracing: &no, IncludeSQL: &yes}
	copied := original.Copy()
	yes, no = false, true
	if !*copied.EnableAudit || *copied.EnableTracing || !*copied.IncludeSQL {
		t.Fatal("policy changed after configuration")
	}
	empty := (&Logging{}).Copy()
	if empty.EnableAudit != nil || empty.EnableTracing != nil || empty.IncludeSQL != nil {
		t.Fatal("unset policy became explicit")
	}
}

func TestRecorderLoggingOptionalPolicy(t *testing.T) {
	absent := &Recorder{}
	if internallog.Enabled(absent) {
		t.Fatal("nil recorder enabled")
	}
	internallog.LogHTTP(absent, nil, nil)
	if internallog.Enabled(NewRecorder(nil)) {
		t.Fatal("native default changed")
	}
	if internallog.Enabled(NewRecorder(nil, WithLogging(nil))) {
		t.Fatal("nil profile enabled")
	}
	if !internallog.Enabled(NewRecorder(nil, WithLogging(&Logging{}))) {
		t.Fatal("default audit disabled")
	}
	no := false
	option := WithLogging(&Logging{EnableAudit: &no, EnableTracing: &no})
	no = true
	if internallog.Enabled(NewRecorder(nil, option)) {
		t.Fatal("configuration changed after option creation")
	}
}

func TestRecorderDoesNotExposeHTTPForwarding(t *testing.T) {
	if _, ok := reflect.TypeOf((*Recorder)(nil)).MethodByName("LogHTTP"); ok {
		t.Fatal("public HTTP forwarding method")
	}
}
