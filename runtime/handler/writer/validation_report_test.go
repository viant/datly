package writer

import (
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestInputValidationReportRetainsNativeAndOrderedBusinessFailures(t *testing.T) {
	value := "private checked input"
	native := &xhandler.Validation{Failed: true, Code: 400, Violations: []*xhandler.Violation{{Location: "Rows[0]", Field: "Name", Message: "too long", Check: "length", CheckedValue: &value, HasCheckedValue: true}}}
	report := &inputValidationReport{native: native}
	evidence := report.SchemaViolations()
	evidence[0].Message = "changed"
	if report.SchemaViolations()[0].Message != "too long" {
		t.Fatal("schema evidence can be mutated")
	}
	report.Add(xhandler.Violation{Location: "Rows[0]", Field: "JSON", Message: "invalid JSON"})
	report.Add(xhandler.Violation{Location: "Rows[1]", Field: "JSON", Message: "required JSON"})
	result := report.result()
	if !report.SchemaFailed() || result.StatusCode() != 400 || len(result.Violations) != 3 {
		t.Fatal("native or business failure lost")
	}
	if result.Violations[0].CheckedValue != &value || result.Violations[1].Message != "invalid JSON" || result.Violations[2].Message != "required JSON" {
		t.Fatal("checked evidence or source order changed")
	}
	if len(native.Violations) != 1 {
		t.Fatal("business additions mutated native report")
	}
}

func TestInputValidationReportEmptyInputAndFailureWithoutDetails(t *testing.T) {
	report := &inputValidationReport{}
	if report.SchemaFailed() || report.result().Err() != nil {
		t.Fatal("empty input must remain valid")
	}
	report.Add(xhandler.Violation{Message: "business failure"})
	if report.result().Err() == nil {
		t.Fatal("business failure was ignored")
	}
	report = &inputValidationReport{native: &xhandler.Validation{Failed: true}}
	if !report.SchemaFailed() || report.result().Err() == nil {
		t.Fatal("native failure without detail was lost")
	}
}
