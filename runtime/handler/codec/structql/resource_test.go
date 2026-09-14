package structql

import (
	"context"
	"reflect"
	"testing"
	"testing/fstest"

	xcodec "github.com/viant/xdatly/codec"
)

func TestFactoryQueryResourcesAndErrors(t *testing.T) {
	type record struct{ ID int }
	type keys struct{ Values []int }
	const sql = "SELECT ARRAY_AGG(ID) AS Values FROM `/` LIMIT 1"
	for _, tt := range []struct {
		name, argument, content string
		resources, reject       bool
	}{
		{"inline", sql, "", false, false}, {"optional prefix", "? " + sql, "", false, false}, {"required prefix", "! " + sql, "", false, false},
		{"resource", "uri=ids.sql", sql, true, false}, {"resource prefix", "uri=ids.sql", "? " + sql, true, false},
		{"missing filesystem", "uri=ids.sql", "", false, true}, {"missing resource", "uri=missing.sql", sql, true, true},
		{"malformed query", "not a query", "", false, true}, {"no source", "SELECT 1", "", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := &xcodec.Config{Body: Name, SourceType: reflect.TypeOf([]record{}), DestinationType: reflect.TypeOf(keys{}), Args: []string{tt.argument}}
			var options []xcodec.Option
			resources := fstest.MapFS{"ids.sql": &fstest.MapFile{Data: []byte(tt.content)}}
			if tt.resources {
				options = append(options, xcodec.WithResourceFS(resources))
			}
			instance, err := (Factory{}).New(config, options...)
			if tt.reject {
				if err == nil {
					t.Fatal("invalid query accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			resources["ids.sql"].Data = []byte("invalid changed resource")
			actual, err := instance.Value(context.Background(), []record{{ID: 1}, {ID: 3}})
			if err != nil || !reflect.DeepEqual(actual, keys{Values: []int{1, 3}}) {
				t.Fatalf("result=%+v error=%v", actual, err)
			}
			if config.Args[0] != tt.argument {
				t.Fatal("factory mutated query config")
			}
		})
	}
}
