package transcribe

import "testing"

func TestConstantTableNameRecognizesOnlyUnquotedSelectors(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: `$Unsafe.Vendor`, want: "Vendor"},
		{input: `${Unsafe.Vendor}`, want: "Vendor"},
		{input: `Unsafe_Vendor`, want: "Vendor"},
		{input: `"$Unsafe.Vendor"`},
		{input: "`$Unsafe.Vendor`"},
		{input: `vendors`},
	}
	for _, test := range tests {
		if actual := constantTableName(test.input); actual != test.want {
			t.Fatalf("constantTableName(%q) = %q, want %q", test.input, actual, test.want)
		}
	}
}
