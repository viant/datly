package provider

import (
	"context"
	"github.com/google/uuid"
	"github.com/viant/bindly/locator"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestGeneratorLegacyValues(t *testing.T) {
	provider := Generator()
	if !provider.(locator.CachePolicy).DefaultCacheable() {
		t.Fatal("legacy cache policy changed")
	}
	value := provider.Locate(nil)
	for _, tc := range []struct {
		name  string
		want  any
		found bool
	}{
		{"nil", nil, true}, {"NiL", nil, true}, {"TRUE", true, true}, {"false", false, true}, {"zero", 0, true}, {"ONE", 1, true}, {"empty", "", true},
		{"", nil, false}, {"unknown", nil, false}, {" current_time", nil, false}, {"now ", nil, false},
	} {
		got, found, err := value.Value(context.Background(), reflect.TypeOf(0), tc.name)
		if err != nil || found != tc.found || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%q=(%v,%v,%v)", tc.name, got, found, err)
		}
	}
	for _, name := range []string{"now", "current_time", "CURRENT_TIME"} {
		start := time.Now()
		v, ok, err := value.Value(context.Background(), nil, name)
		end := time.Now()
		stamp, typed := v.(time.Time)
		if err != nil || !ok || !typed || stamp.Before(start) || stamp.After(end) {
			t.Fatalf("invalid clock %q", name)
		}
	}
	var wg sync.WaitGroup
	ids := make(chan string, 64)
	for j := 0; j < 64; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok, err := value.Value(context.Background(), nil, "uuid")
			if err != nil || !ok {
				t.Error("uuid absent")
				return
			}
			text := v.(string)
			id, err := uuid.Parse(text)
			if err != nil || id.Version() != 4 || id.String() != text {
				t.Error("invalid UUID")
			}
			ids <- text
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Error("UUID retained across resolutions")
		}
		seen[id] = true
	}
}
