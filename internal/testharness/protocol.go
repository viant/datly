package testharness

import "testing"

// StructuredObject asserts an object-shaped structured protocol payload. MCP's
// schema permits any JSON value; tests of object contracts must prove the shape
// before indexing it.
func StructuredObject(t testing.TB, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok || object == nil {
		t.Fatalf("expected structured JSON object, got %T (%#v)", value, value)
	}
	return object
}
