package command

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
)

func TestCollectPlannedSQLAssets_AbsolutizesAndDedupes(t *testing.T) {
	tempDir := t.TempDir()
	source := &shape.Source{Path: filepath.Join(tempDir, "query.dql")}
	planned := &plan.Result{
		Components: []*plan.ComponentRoute{
			{SourceURL: "foo/root.sql"},
		},
		Views: []*plan.View{
			{Name: "foo", SQLURI: "foo/root.sql"},
			{Name: "bar", SQLURI: "bar/detail.sql", SummaryURL: "bar/summary.sql"},
		},
	}

	assets := collectPlannedSQLAssets(source, planned)

	require.Len(t, assets, 3)
	require.Contains(t, assets[0]+assets[1]+assets[2], filepath.ToSlash(filepath.Join(tempDir, "foo", "root.sql")))
	require.Contains(t, assets[0]+assets[1]+assets[2], filepath.ToSlash(filepath.Join(tempDir, "bar", "detail.sql")))
	require.Contains(t, assets[0]+assets[1]+assets[2], filepath.ToSlash(filepath.Join(tempDir, "bar", "summary.sql")))
}

func TestValidatePlannedSQLAssets_MissingPath(t *testing.T) {
	tempDir := t.TempDir()
	source := &shape.Source{Path: filepath.Join(tempDir, "query.dql")}
	planned := &shape.PlanResult{Source: source, Plan: &plan.Result{
		Views: []*plan.View{
			{Name: "foo", SQLURI: "foo/missing.sql"},
		},
	}}

	svc := New()
	err := validatePlannedSQLAssets(context.Background(), svc, source, planned)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing SQL asset")
	require.Contains(t, err.Error(), "missing.sql")
}

func TestValidate_PatchBasicOne(t *testing.T) {
	projectDir, err := filepath.Abs(filepath.Join("..", "..", "e2e", "v1"))
	require.NoError(t, err)
	source := filepath.Join(projectDir, "dql", "dev", "events", "patch_basic_one.dql")
	svc := New()

	err = svc.Validate(context.Background(), &options.Options{
		Validate: &options.Validate{
			Project: projectDir,
			Source:  []string{source},
		},
	})

	require.NoError(t, err)
}
