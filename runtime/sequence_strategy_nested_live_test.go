package runtime

import (
	"testing"

	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/testutil/reservationdb"
)

func TestRuntimeOmittedRootNativeDefaultLive(t *testing.T) {
	for _, testCase := range []struct {
		driver    string
		matching  string
		differing string
	}{
		{driver: "mysql", matching: "transient", differing: "reservation"},
		{driver: "postgres", matching: "reservation", differing: "transient"},
	} {
		t.Run(testCase.driver, func(t *testing.T) {
			if testCase.driver == "mysql" {
				database := reservationdb.OpenTransient(t)
				assertRuntimeSequenceStrategy(t, dml.Source{DB: database.DB}, testCase.matching, testCase.differing)
				return
			}
			database := reservationdb.Open(t, testCase.driver)
			assertRuntimeSequenceStrategy(t, dml.Source{DB: database.DB}, testCase.matching, testCase.differing)
		})
	}
}
