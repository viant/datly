package jobs

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"testing"
)

func TestJobTableDialectConfiguration(t *testing.T) {
	for _, tc := range []struct{ product, dataset, table, want string }{{"MySQL", "jobs_db", "DATLY_JOBS", "`jobs_db`.`DATLY_JOBS`"}, {"BigQuery", "project-id.dataset", "DATLY_JOBS", "`project-id.dataset.DATLY_JOBS`"}, {"PostgreSQL", "jobs", "DATLY_JOBS", `"jobs"."DATLY_JOBS"`}, {"SQLite", "main", "DATLY_JOBS", `"main"."DATLY_JOBS"`}} {
		t.Run(tc.product, func(t *testing.T) {
			got, err := (SQLConfig{Dataset: tc.dataset, Table: tc.table}).table(&info.Dialect{Product: database.Product{Name: tc.product}})
			if err != nil || got != tc.want {
				t.Fatalf("table=%s err=%v", got, err)
			}
		})
	}
	if _, err := (SQLConfig{Table: "DATLY_JOBS; DELETE FROM users"}).table(&info.Dialect{}); err == nil {
		t.Fatal("executable table configuration accepted")
	}
	if _, err := (SQLConfig{Dataset: "jobs", Table: "other.DATLY_JOBS"}).table(&info.Dialect{}); err == nil {
		t.Fatal("ambiguous qualification accepted")
	}
}
