package dml

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

// An explicit opt-in is required; ordinary unit tests never discover a MySQL DSN.
func TestNativeBatchUpsertMySQLDefaultsAndConflict(t *testing.T) {
	dsn := os.Getenv("PLATFORM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("PLATFORM_TEST_MYSQL_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("open MySQL failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Temporary table belongs only to this connection and vanishes on close.
	_, err = db.ExecContext(ctx, `CREATE TEMPORARY TABLE datly_native_upsert (AUDIENCE_ID BIGINT NOT NULL,CREATIVE_ID BIGINT NOT NULL,WEIGHT DOUBLE NOT NULL,CREATED_USER VARCHAR(40) NOT NULL,UPDATED_USER VARCHAR(40) NOT NULL,CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,UPDATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(AUDIENCE_ID,CREATIVE_ID))`)
	if err != nil {
		t.Fatal("temporary fixture creation failed")
	}
	_, err = db.ExecContext(ctx, `INSERT INTO datly_native_upsert(AUDIENCE_ID,CREATIVE_ID,WEIGHT,CREATED_USER,UPDATED_USER,CREATED,UPDATED) VALUES(1,1,2,'original','old','2001-01-01 00:00:00','2001-01-01 00:00:00')`)
	if err != nil {
		t.Fatal("fixture insert failed")
	}
	d := NewData(db)
	d.dialect = registry.LookupDialect(mysqlmeta.MySQL5())
	if err = d.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	rows := []*upsertWeight{{1, 1, 7, "replacement", "new"}, {1, 2, 9, "new creator", "new"}}
	var affected int64
	if err = d.InsertWithBatchSizeAndOnDuplicateKeySQL("datly_native_upsert", rows, len(rows), &affected, audienceConflictSQL); err != nil {
		t.Fatal(err)
	}
	if err = d.Complete(ctx, nil); err != nil {
		t.Fatal("native upsert failed")
	}
	var weight float64
	var creator, updater, created string
	var recent bool
	err = db.QueryRowContext(ctx, `SELECT WEIGHT,CREATED_USER,UPDATED_USER,CAST(CREATED AS CHAR),UPDATED >= CURRENT_TIMESTAMP - INTERVAL 1 MINUTE FROM datly_native_upsert WHERE AUDIENCE_ID=1 AND CREATIVE_ID=1`).Scan(&weight, &creator, &updater, &created, &recent)
	if err != nil || weight != 7 || creator != "original" || updater != "new" || created != "2001-01-01 00:00:00" || !recent {
		t.Fatal("conflict update/default preservation failed")
	}
	var freshCreated, freshUpdated bool
	err = db.QueryRowContext(ctx, `SELECT WEIGHT,CREATED_USER,UPDATED_USER,CREATED >= CURRENT_TIMESTAMP - INTERVAL 1 MINUTE,UPDATED >= CURRENT_TIMESTAMP - INTERVAL 1 MINUTE FROM datly_native_upsert WHERE AUDIENCE_ID=1 AND CREATIVE_ID=2`).Scan(&weight, &creator, &updater, &freshCreated, &freshUpdated)
	if err != nil || weight != 9 || creator != "new creator" || updater != "new" || !freshCreated || !freshUpdated {
		t.Fatal("new row database defaults failed")
	}
}
