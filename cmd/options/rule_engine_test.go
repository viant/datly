package options

import "testing"

func TestRule_EffectiveEngine(t *testing.T) {
	testCases := []struct {
		name   string
		engine string
		want   string
	}{
		{name: "default", engine: "", want: EngineLegacy},
		{name: "shape", engine: "shape", want: EngineShape},
		{name: "invalid", engine: "other", want: EngineLegacy},
	}
	for _, testCase := range testCases {
		rule := &Rule{Engine: testCase.engine}
		if got := rule.EffectiveEngine(); got != testCase.want {
			t.Fatalf("%s: got %s, want %s", testCase.name, got, testCase.want)
		}
	}
}
