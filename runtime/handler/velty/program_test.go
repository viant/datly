package velty

import (
	"errors"
	"strings"
	"testing"
)

type ProgramTestState struct{}

type programTestInput struct {
	Name string
}

type programFailureProbe struct{ calls []string }

func (p *programFailureProbe) First() (string, error) {
	p.calls = append(p.calls, "first")
	return "", errors.New("stop here")
}
func (p *programFailureProbe) Second() string { p.calls = append(p.calls, "second"); return "" }

type programFailureInput struct {
	Probe *programFailureProbe
	Name  string
}

type programFailureDML struct {
	capabilityDML
	inserts int
}

func (d *programFailureDML) Insert(string, any) error { d.inserts++; return nil }

func TestCompiledProgramStopsOnCapabilityError(t *testing.T) {
	program, err := newCompiledProgram[Context, programFailureInput, programTestOutput](`$Input.Probe.First()$dml.Insert("records", $Input)$Input.Probe.Second()`)
	if err != nil {
		t.Fatal(err)
	}
	probe := &programFailureProbe{}
	buffer := &programFailureDML{}
	err = program.Exec(&Context{DML: &DML{data: buffer}}, &programFailureInput{Probe: probe}, &programTestOutput{})
	if err == nil || !strings.Contains(err.Error(), "stop here") {
		t.Fatalf("error %v", err)
	}
	if strings.Join(probe.calls, ",") != "first" {
		t.Fatalf("execution continued after error: %v", probe.calls)
	}
	if buffer.inserts != 0 {
		t.Fatal("DML queued after first capability error")
	}
}

type programTestOutput struct {
	Name  string
	Lower string
	Field string
}

func TestCompiledProgramExposesTypedInputAliases(t *testing.T) {
	program, err := newCompiledProgram[ProgramTestState, programTestInput, programTestOutput](`
#set($Output.Name = $Input.Name)
#set($Output.Lower = $input.Name)
#set($Output.Field = $Name)`)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	input := &programTestInput{Name: "Ada"}
	output := &programTestOutput{}
	if err := program.Exec(&ProgramTestState{}, input, output); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if output.Name != "Ada" || output.Lower != "Ada" || output.Field != "Ada" {
		t.Fatalf("expected typed input aliases, got %+v", output)
	}
}

type GenericProgramState[T any] struct {
	Value T
}

func TestCompiledProgramConvertsPlannerPanicToError(t *testing.T) {
	_, err := newCompiledProgram[GenericProgramState[int], programTestInput, programTestOutput](`$Input.Name`)
	if err == nil || !strings.Contains(err.Error(), "compile velty program") {
		t.Fatalf("expected registration-safe compile error, got %v", err)
	}
}
