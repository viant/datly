package structql

import (
	xcodec "github.com/viant/xdatly/codec"
	"reflect"
	"testing"
)

func BenchmarkLinkedStructQLCompile(b *testing.B) {
	type event struct{ Id int }
	type result struct{ Values []int }
	config := &xcodec.Config{Body: Name, SourceType: reflect.TypeFor[[]event](), DestinationType: reflect.TypeFor[result](), Args: []string{"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1"}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := (Factory{}).New(config); err != nil {
			b.Fatal(err)
		}
	}
}
func TestLinkedStructQLRejectsInvalidQueries(t *testing.T) {
	type row struct{ Id int }
	for _, sql := range []string{"", "SELECT", "SELECT FROM `/`", "SELECT Id", "DELETE FROM `/`", "SELECT Id FROM", "SELECT ( FROM `/`"} {
		t.Run(sql, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("panic: %v", p)
				}
			}()
			_, err := (Factory{}).New(&xcodec.Config{Body: Name, SourceType: reflect.TypeFor[[]row](), DestinationType: reflect.TypeFor[[]row](), Args: []string{sql}})
			if err == nil {
				t.Fatal("invalid SQL accepted")
			}
		})
	}
}
