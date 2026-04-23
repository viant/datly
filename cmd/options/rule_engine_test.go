package options

import "testing"

func TestRule_EffectiveEngine(t *testing.T) {
	testCases := []struct {
		name   string
		engine string
		want   string
	}{
		{name: "default", engine: "", want: EngineInternal},
		{name: "internal", engine: "internal", want: EngineInternal},
		{name: "legacy alias", engine: "legacy", want: EngineInternal},
		{name: "shape", engine: "shape", want: EngineShape},
		{name: "shape ir", engine: "shape-ir", want: EngineShapeIR},
		{name: "shape ir alias", engine: "shapeir", want: EngineShapeIR},
		{name: "invalid", engine: "other", want: EngineInternal},
	}
	for _, testCase := range testCases {
		rule := &Rule{Engine: testCase.engine}
		if got := rule.EffectiveEngine(); got != testCase.want {
			t.Fatalf("%s: got %s, want %s", testCase.name, got, testCase.want)
		}
	}
}
