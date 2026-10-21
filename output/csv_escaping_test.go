package output

import (
	"bytes"
	"context"
	"encoding/csv"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCSVStandardEscaping(t *testing.T) {
	type row struct {
		ID    int    `csvName:"id"`
		Value string `csvName:"value"`
	}
	input := []row{{1, `say "hello", please`}, {2, `C:\folder\file`}, {3, "one\ntwo"}, {4, "one\r\ntwo"}, {5, "日本語"}, {6, ""}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[[]row]()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := plan.Encode(context.Background(), "csv", input)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContentType != "text/csv" {
		t.Fatal(result.ContentType)
	}
	rows, err := csv.NewReader(bytes.NewReader(result.Data)).ReadAll()
	if err != nil {
		t.Fatalf("native CSV is not standard CSV: %v\n%s", err, result.Data)
	}
	if !reflect.DeepEqual(rows[0], []string{"id", "value"}) {
		t.Fatal(rows[0])
	}
	for i, expected := range input {
		if rows[i+1][1] != strings.ReplaceAll(expected.Value, "\r\n", "\n") {
			t.Fatalf("row %d: %q", i, rows[i+1][1])
		}
	}
}
func TestCSVConcurrentNativePlan(t *testing.T) {
	type row struct{ Value string }
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[[]row]()})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 24; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			value := `a,"b"\c`
			for n := 0; n < 5; n++ {
				result, err := plan.Encode(context.Background(), "csv", []row{{value}})
				if err != nil {
					t.Error(err)
					return
				}
				records, err := csv.NewReader(bytes.NewReader(result.Data)).ReadAll()
				if err != nil || len(records) != 2 || records[1][0] != value {
					t.Errorf("native CSV race: %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
}
