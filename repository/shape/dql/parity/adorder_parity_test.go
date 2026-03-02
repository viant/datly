package parity

import (
	"context"
	"os"
	"strings"
	"testing"

	dqlplan "github.com/viant/datly/repository/shape/dql/plan"
	dqlyaml "github.com/viant/datly/repository/shape/dql/render/yaml"
	dqlscan "github.com/viant/datly/repository/shape/dql/scan"
)

func TestAdorderDQL_CanonicalParityWithYAML(t *testing.T) {
	if os.Getenv("DATLY_RUN_ADORDER_PARITY") != "1" {
		t.Skip("set DATLY_RUN_ADORDER_PARITY=1 to run adorder parity suite")
	}
	dqlPath := "/Users/adrianwitas/Downloads/pp/dql/platform/adorder/adorder.dql"
	yamlPath := "/Users/adrianwitas/Downloads/pp/repo/dev/Datly/routes/platform/adorder/adorder.yaml"
	repoPath := "/Users/adrianwitas/Downloads/pp/repo/dev"

	if _, err := os.Stat(dqlPath); err != nil {
		t.Skipf("missing fixture dql file: %v", err)
	}
	if _, err := os.Stat(yamlPath); err != nil {
		t.Skipf("missing fixture yaml file: %v", err)
	}

	scanner := dqlscan.New()
	connectors := resolveConnectors([]string{
		"ci_ads|mysql|root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true&charset=utf8mb4&collation=utf8mb4_bin",
		"ci_logs|mysql|root:dev@tcp(127.0.0.1:3307)/ci_logs?parseTime=true",
	})
	scanned, err := scanner.Scan(context.Background(), &dqlscan.Request{
		DQLURL:       dqlPath,
		Repository:   repoPath,
		ModulePrefix: "platform/adorder",
		APIPrefix:    "/v1/api",
		Connectors:   connectors,
	})
	if err != nil {
		if strings.Contains(err.Error(), "Unknown database") || strings.Contains(err.Error(), "failed to discover/detect column") {
			t.Skipf("environment not ready for parity scan: %v", err)
		}
		t.Fatalf("scan failed: %v", err)
	}

	fromDQL, err := dqlplan.BuildFromIR(scanned.IR)
	if err != nil {
		t.Fatalf("plan from dql failed: %v", err)
	}

	yamlData, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Fatalf("read yaml failed: %v", err)
	}
	fromYAML, err := dqlplan.Build(yamlData)
	if err != nil {
		t.Fatalf("plan from yaml failed: %v", err)
	}
	issues := Diff(fromDQL.Canonical, fromYAML.Canonical)
	if len(issues) > 0 {
		max := len(issues)
		if max > 30 {
			max = 30
		}
		for i := 0; i < max; i++ {
			t.Log(issues[i])
		}
		t.Fatalf("canonical diff detected: %d issues", len(issues))
	}

	renderedYAML, err := dqlyaml.Encode(scanned.IR)
	if err != nil {
		t.Fatalf("render yaml from IR failed: %v", err)
	}
	fromRendered, err := dqlplan.Build(renderedYAML)
	if err != nil {
		t.Fatalf("plan from rendered yaml failed: %v", err)
	}
	roundTripIssues := Diff(fromRendered.Canonical, fromYAML.Canonical)
	if len(roundTripIssues) > 0 {
		max := len(roundTripIssues)
		if max > 30 {
			max = 30
		}
		for i := 0; i < max; i++ {
			t.Log(roundTripIssues[i])
		}
		t.Fatalf("ir->yaml canonical diff detected: %d issues", len(roundTripIssues))
	}
}
