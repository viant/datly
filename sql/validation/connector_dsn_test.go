package validation

import (
	"context"
	"testing"

	"github.com/viant/govalidator"
)

func TestConnectorDSNValidation(t *testing.T) {
	type connector struct {
		Driver    *string
		SecretRef *string
		DSN       *string `validate:"datly_connector_dsn(Driver,SecretRef)"`
	}
	value := func(source string) *string { return &source }
	tests := []struct {
		name, driver, dsn, secret string
		valid                     bool
	}{
		{"mysql", "mysql", "user:pass@tcp(localhost:3306)/studio", "", true},
		{"bigquery", "viant/bigquery", "bigquery://project/dataset", "", true},
		{"pg URL", "pg", "postgres://user:pass@localhost:5432/studio", "", true},
		{"pg keyword", "pg", "host=localhost dbname=studio user=test", "", true},
		{"sqlite file", "sqlite", "file:studio.db?cache=shared", "", true},
		{"sqlite memory", "sqlite", ":memory:", "", true},
		{"aerospike", "viant/aerospike", "aerospike://localhost:3000/studio", "", true},
		{"secret reference", "mysql", "", "secret://studio/mysql", true},
		{"unsupported", "oracle", "oracle://localhost/studio", "", false},
		{"mismatched", "sqlite", "bigquery://project/dataset", "", false},
		{"missing", "mysql", "", "", false},
	}
	validator := govalidator.New()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := &connector{Driver: value(test.driver), DSN: value(test.dsn)}
			if test.secret != "" {
				candidate.SecretRef = value(test.secret)
			}
			actual, err := validator.Validate(context.Background(), candidate)
			if err != nil {
				t.Fatal(err)
			}
			if valid := !actual.Failed; valid != test.valid {
				t.Fatalf("valid=%v violations=%+v", valid, actual.Violations)
			}
		})
	}
}
