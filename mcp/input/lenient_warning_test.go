package input

import (
	"bytes"
	bindstate "github.com/viant/bindly/state"
	"log/slog"
	"reflect"
	"strings"
	"testing"
)

func TestLenientArgumentWarningStaysServerSide(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	argument := Argument{PublicName: "id", Source: bindstate.Location{Kind: "query", In: "id"}, SourceType: reflect.TypeOf(int(0))}
	plan, err := NewCompiler().Compile([]Argument{argument})
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := plan.NormalizeArguments(map[string]any{"id": 7, "extra": "not-for-logs"})
	if err != nil || len(normalized) != 1 || normalized["id"] != 7 {
		t.Fatalf("lenient projection changed: %v %v", normalized, err)
	}
	if !strings.Contains(logs.String(), `"argument":"extra"`) || strings.Contains(logs.String(), "not-for-logs") {
		t.Fatalf("warning missing or retained an argument value: %s", logs.String())
	}
	strict, err := NewCompiler(WithStrictArguments(true)).Compile([]Argument{argument})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = strict.NormalizeArguments(map[string]any{"id": 7, "extra": false}); err == nil {
		t.Fatal("strict opt-in no longer rejects extra arguments")
	}
}
