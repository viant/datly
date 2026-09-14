package transcribe

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
)

func TestCompilerTranscribesExplicitVeltyServiceProgram(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/generated/events", Name: "Events",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Name<string>(body/Name))
#define($_ = $Result<string>(output/body))
$dml.Execute("INSERT INTO events(name) VALUES (?)", $Name);
#set($Output.Result = $Name)`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if result.VeltyHandler == nil || !strings.Contains(result.VeltyHandler.Template, "$dml.Execute") ||
		!strings.Contains(result.VeltyHandler.Template, "$Output.Result") {
		t.Fatalf("VeltyHandler = %#v", result.VeltyHandler)
	}
	classification := result.Statements.Classify()
	if !classification.HasService || !classification.HasUnknown || classification.HasRead || classification.HasExec {
		t.Fatalf("statement classification = %+v", classification)
	}
}

func TestCompilerTranscribesComprehensiveManyVeltyStructure(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/generated/events", Name: "EventsMany",
		Text: `#setting($_ = $route('/events-many', 'POST'))
#define($_ = $Events<[]Event>(body/Data).Required())
#define($_ = $Data<[]Event>(output/body))
$sequencer.Allocate("EVENTS", $Events, "ID")
#foreach($Event in $Events)
$dml.Insert("EVENTS", $Event);
#end
#set($Output.Data = $Events)`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if result.VeltyHandler == nil || !strings.Contains(result.VeltyHandler.Template, "$sequencer.Allocate") ||
		!strings.Contains(result.VeltyHandler.Template, "#foreach") ||
		!strings.Contains(result.VeltyHandler.Template, "$dml.Insert") ||
		!strings.Contains(result.VeltyHandler.Template, "$Output.Data") {
		t.Fatalf("VeltyHandler = %#v", result.VeltyHandler)
	}
	classification := result.Statements.Classify()
	if !classification.HasService || !classification.HasUnknown || classification.HasRead || classification.HasExec {
		t.Fatalf("statement classification = %+v", classification)
	}
}

func TestTranscribeVeltyHandlerExcludesRawAndMixedSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "raw DML", sql: "INSERT INTO events(name) VALUES ('x')"},
		{name: "mixed read", sql: "$dml.Execute(\"DELETE FROM events\"); SELECT * FROM events"},
		{name: "mixed raw DML", sql: "$dml.Delete(\"events\", $Event); UPDATE audit SET changed = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared := &dql.PreparedSource{SQL: test.sql, Statements: statement.Parse(test.sql)}
			actual, err := transcribeVeltyHandler(prepared, nil)
			if err != nil || actual != nil {
				t.Fatalf("transcribeVeltyHandler() = (%#v, %v)", actual, err)
			}
		})
	}
}

func TestCompilerRejectsUnsafeVeltyServicePrograms(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "raw DML without delimiter", body: "$dml.Insert(\"events\", $Event)\nUPDATE audit SET changed = 1"},
		{name: "read without delimiter", body: "$dml.Insert(\"events\", $Event)\nSELECT * FROM audit"},
		{name: "unsupported SQL", body: "$dml.Insert(\"events\", $Event); CREATE TABLE x(id INT)"},
		{name: "unknown selector", body: "$dml.Insert(\"events\", $Event); $unknown.Call($Event)"},
		{name: "DML method reference", body: "$dml.Insert"},
		{name: "DML method selector", body: "$dml.Insert.Name"},
		{name: "value-first insert", body: "$dml.Insert($Event, \"events\")"},
		{name: "missing insert value", body: "$dml.Insert(\"events\")"},
		{name: "extra insert argument", body: "$dml.Insert(\"events\", $Event, $Event)"},
		{name: "missing execute statement", body: "$dml.Execute()"},
		{name: "dynamic execute statement", body: "$dml.Execute($Event)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewCompiler().Compile(context.Background(), &Source{
				Name: "Events",
				Text: "#setting($_ = $route('/events', 'POST'))\n" +
					"#define($_ = $Event<Event>(body/Data))\n" + test.body,
			})
			var compileError *CompileError
			if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
				compileError.Diagnostics[0].Code != "DQL-HANDLER" {
				t.Fatalf("Compile() error = %#v", err)
			}
		})
	}
}

func TestCompilerTranscribesDMLCapabilityAlias(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Event<Event>(body/Data))
$dml.Insert("events", $Event)`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if result.VeltyHandler == nil || !strings.Contains(result.VeltyHandler.Template, "$dml.Insert") {
		t.Fatalf("VeltyHandler = %#v", result.VeltyHandler)
	}
}

func TestCompilerMapsVeltyHandlerDiagnosticToAuthoredSelector(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events", Path: "/tmp/events.dql",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Event<Event>(body/Data))
$dml.Insert("events", $Event);
$unknown.Call($Event)`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != "DQL-HANDLER" || diagnostic.Path != "/tmp/events.dql" ||
		diagnostic.Span.Start.Line != 4 || diagnostic.Span.Start.Char != 1 || diagnostic.Span.End.Char <= diagnostic.Span.Start.Char {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerMapsVeltyHandlerArgumentDiagnosticToAuthoredSelector(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events", Path: "/tmp/events.dql",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Event<Event>(body/Data))
$dml.Insert("events", $unknown)`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != "DQL-HANDLER" || diagnostic.Span.Start.Line != 3 ||
		diagnostic.Span.Start.Char != 23 || diagnostic.Span.End.Char != 31 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}
