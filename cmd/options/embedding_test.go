package options

import "testing"

// Unit tests for extractAsset function
func TestExtractAsset(t *testing.T) {
	tests := []struct {
		input    string
		expected *embedding
		err      error
	}{
		{"yyy${embed:path/assetxx.sss}xxx", &embedding{"${embed:path/assetxx.sss}", "path/assetxx.sss", nil}, nil},
		{"yyy${embed({\"k1\":1}):path/assetxx.sss}xxx", &embedding{"${embed({\"k1\":1}):path/assetxx.sss}", "path/assetxx.sss", map[string]interface{}{"k1": float64(1)}}, nil},
		{"Some text without embed", &embedding{}, nil},
	}

	for _, test := range tests {
		result, err := newEmbedding(test.input)
		if result != test.expected && err != nil && err.Error() != test.err.Error() {
			t.Errorf("For input '%s', expected '%v' with error '%v', but got '%v' with error '%v'", test.input, test.expected, test.err, result, err)
		}
	}
}
