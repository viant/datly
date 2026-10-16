package exec_test

import (
	"errors"
	"github.com/viant/datly/exec"
	"github.com/viant/xdatly/handler"
	"testing"
)

type legacyReporter struct{ report exec.MutationReport }

func (r legacyReporter) MutationReport() exec.MutationReport { return r.report }

var _ handler.MutationReporter = legacyReporter{}

func TestLegacyPublicContractCompatibility(t *testing.T) {
	cause := errors.New("driver failure")
	var public handler.MutationReporter = legacyReporter{report: exec.MutationReport{Queued: 1, Results: []exec.MutationResult{{Operation: "UPDATE", Records: 1, Error: cause}}}}
	var legacy exec.MutationReporter = public
	if report := legacy.MutationReport(); report.Queued != 1 || len(report.Results) != 1 || !errors.Is(report.Results[0].Error, cause) {
		t.Fatalf("report=%+v", report)
	}
	input := new(struct{ ID int })
	legacySeed := exec.WithInput(input)
	var publicSeed *handler.InputSeed = legacySeed
	var roundTrip *exec.InputSeed = publicSeed
	if roundTrip.Value() != input {
		t.Fatal("typed input identity changed")
	}
	if exec.MutationReporterKey != handler.MutationReporterKey {
		t.Fatal("binding key changed")
	}
}
