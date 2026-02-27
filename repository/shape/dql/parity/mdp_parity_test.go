package parity

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dqlplan "github.com/viant/datly/repository/shape/dql/plan"
	dqlscan "github.com/viant/datly/repository/shape/dql/scan"
)

func TestMDPDQL_CanonicalParityWithRoutes(t *testing.T) {
	if os.Getenv("DATLY_RUN_MDP_PARITY") != "1" {
		t.Skip("set DATLY_RUN_MDP_PARITY=1 to run mdp parity suite")
	}
	mdpRoot := envOr("DATLY_MDP_ROOT", "/Users/adrianwitas/go/src/github.vianttech.com/viant/mdp")
	repoRoot := envOr("DATLY_MDP_REPO", filepath.Join(mdpRoot, "repo", "dev"))
	routesRoot := filepath.Join(repoRoot, "Datly", "routes", "mdp")
	dqlRoot := filepath.Join(mdpRoot, "dql")
	if _, err := os.Stat(routesRoot); err != nil {
		t.Fatalf("routes root missing: %v", err)
	}
	if _, err := os.Stat(dqlRoot); err != nil {
		t.Fatalf("dql root missing: %v", err)
	}

	connectors := splitNonEmpty(os.Getenv("DATLY_MDP_CONNECTORS"))
	if len(connectors) == 0 {
		connectors = resolveConnectors([]string{
			"ci_ads|mysql|root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true&charset=utf8mb4&collation=utf8mb4_bin",
			"ci_ads_rw|mysql|root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true&charset=utf8mb4&collation=utf8mb4_bin",
			"bq_mdp|mysql|root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true&charset=utf8mb4&collation=utf8mb4_bin",
			"bq_automation|mysql|root:dev@tcp(127.0.0.1:3307)/ci_ads?parseTime=true&charset=utf8mb4&collation=utf8mb4_bin",
		})
	}

	type issue struct {
		route string
		msg   string
	}
	var issues []issue
	scanner := dqlscan.New()
	_ = filepath.WalkDir(routesRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		if filepath.Ext(path) != ".yaml" {
			return nil
		}
		base := filepath.Base(path)
		if base == "producer.yaml" || strings.HasPrefix(path, filepath.Join(routesRoot, ".meta")) || strings.Contains(path, string(filepath.Separator)+".meta"+string(filepath.Separator)) {
			return nil
		}
		rel, err := filepath.Rel(routesRoot, path)
		if err != nil {
			issues = append(issues, issue{route: path, msg: "failed to compute relative route path: " + err.Error()})
			return nil
		}
		ruleDir := filepath.Dir(rel)
		ruleName := strings.TrimSuffix(filepath.Base(path), ".yaml")
		dqlFile := filepath.Join(dqlRoot, ruleDir, ruleName+".dql")
		if _, err = os.Stat(dqlFile); err != nil {
			dqlFile = filepath.Join(dqlRoot, ruleDir, ruleName+".sql")
		}
		if _, err = os.Stat(dqlFile); err != nil {
			t.Logf("skip %s: missing dql/sql counterpart", path)
			return nil
		}
		modulePrefix := filepath.ToSlash(filepath.Join("mdp", ruleDir))
		scanned, err := scanner.Scan(context.Background(), &dqlscan.Request{
			DQLURL:       dqlFile,
			Repository:   repoRoot,
			ModulePrefix: modulePrefix,
			APIPrefix:    "/v1/api",
			Connectors:   connectors,
		})
		if err != nil {
			if strings.Contains(err.Error(), "failed to parse import statement") {
				t.Logf("skip %s: %v", path, err)
				return nil
			}
			issues = append(issues, issue{route: path, msg: "scan failed: " + err.Error()})
			return nil
		}
		fromDQL, err := dqlplan.BuildFromIR(scanned.IR)
		if err != nil {
			issues = append(issues, issue{route: path, msg: "build from dql ir failed: " + err.Error()})
			return nil
		}
		yamlBytes, err := os.ReadFile(path)
		if err != nil {
			issues = append(issues, issue{route: path, msg: "read route yaml failed: " + err.Error()})
			return nil
		}
		fromYAML, err := dqlplan.Build(yamlBytes)
		if err != nil {
			issues = append(issues, issue{route: path, msg: "build from route yaml failed: " + err.Error()})
			return nil
		}
		normalizeMDPCanonical(fromDQL.Canonical)
		normalizeMDPCanonical(fromYAML.Canonical)
		diff := Diff(fromDQL.Canonical, fromYAML.Canonical)
		if len(diff) > 0 {
			msg := "canonical diff issues: " + diff[0]
			issues = append(issues, issue{route: path, msg: msg})
		}
		return nil
	})

	if len(issues) == 0 {
		return
	}
	limit := len(issues)
	if limit > 40 {
		limit = 40
	}
	for i := 0; i < limit; i++ {
		t.Logf("%s => %s", issues[i].route, issues[i].msg)
	}
	t.Fatalf("mdp parity issues: %d", len(issues))
}

func normalizeMDPCanonical(canonical map[string]any) {
	routes, ok := canonical["Routes"].([]any)
	if !ok {
		return
	}
	for _, routeItem := range routes {
		route, ok := routeItem.(map[string]any)
		if !ok {
			continue
		}
		input, ok := route["Input"].(map[string]any)
		if !ok {
			continue
		}
		delete(input, "Parameters")
	}
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitNonEmpty(csv string) []string {
	var ret []string
	for _, item := range strings.Split(csv, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		ret = append(ret, item)
	}
	return ret
}
