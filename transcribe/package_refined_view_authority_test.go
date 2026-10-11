package transcribe

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

func TestPackageIndependentRowAuthoritySurvivesColumnRefinement(t *testing.T) {
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "model/audit.go", "package model\ntype AuditRow struct{ID int}\nfunc init(){panic(\"application initialization executed\")}\n")
	writeSourceFile(t, base, "svc/users/component.go", discoveredPackageComponent)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE users(ID INTEGER)", "CREATE TABLE audit(ID INTEGER)"))
	project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/generated/svc/users"}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}).Compile(context.Background())
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
	compiled := project.Components[0]
	for _, view := range compiled.Component.Views {
		identity, err := view.Identity()
		require.NoError(t, err)
		original := newPackageViewResolver(compiled.Source.PackageComponent, nil).byIdentity(identity)
		if original == nil {
			continue
		}
		before, _ := json.Marshal(original)
		after, _ := json.Marshal(view)
		require.NotNil(t, compiled.Views[identity], "linked authority lost; package=%s refined=%s", before, after)
	}
}
