package engine

import (
	"strings"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/sqlx/metadata/info"
	mconfig "github.com/viant/sqlx/metadata/product/mysql/merge/config"
	"github.com/viant/sqlx/moption"
)

type mergeCapabilityProbe struct {
	matchedDMLProbe
	calls   int
	config  info.MergeConfig
	result  *info.MergeResult
	rows    any
	options []moption.Option
}

func (p *mergeCapabilityProbe) Merge(table string, rows any, config info.MergeConfig, result *info.MergeResult, options ...moption.Option) error {
	p.calls++
	p.table = table
	p.rows = rows
	p.config = config
	p.result = result
	p.options = options
	return nil
}
func TestFocusedMergeDMLPreservesConfigAndRejectsClosedAuthority(t *testing.T) {
	probe := &mergeCapabilityProbe{}
	guard := &mutationGuard{}
	capability := dmlCapability{service: probe, guard: guard}
	var contract rhandler.MergeDML = capability
	config := &mconfig.Config{HighwayHash: true, FetchConcurrency: 5, CompareConcurrency: 6}
	rows := &[]int{1}
	var result info.MergeResult
	option := moption.WithCommonOptions(nil)
	if err := contract.Merge("original_table", rows, config, &result, option); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 1 || probe.table != "original_table" || probe.rows != rows || probe.config != config || probe.result != &result || len(probe.options) != 1 {
		t.Fatal("merge contract was changed")
	}
	guard.completionClosed = true
	if err := contract.Merge("original_table", rows, config, &result); err == nil || probe.calls != 1 {
		t.Fatalf("closed merge reached provider: %v calls=%d", err, probe.calls)
	}
	if err := (dmlCapability{service: &matchedDMLProbe{}}).Merge("original_table", rows, config, &result); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("unsupported provider silently substituted merge: %v", err)
	}
}
