package velty

import "testing"

func TestInspectProgramCapabilitiesUsesRootSelectorsOnly(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     programCapabilities
	}{
		{
			name:     "capability named fields",
			template: `$Input.dml $Output.logger $local.sequencer $record.validator $item.messageBus $record.index`,
		},
		{
			name:     "root capability",
			template: `$dml.Execute("UPDATE T SET A = ?", $Input.logger)`,
			want:     programCapabilities{dml: true},
		},
		{
			name:     "capability in call argument",
			template: `$Input.Apply($validator.Check($Input))`,
			want:     programCapabilities{validator: true},
		},
		{
			name:     "local index",
			template: `$index.Has("events", $Input.Record)`,
			want:     programCapabilities{index: true},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := inspectProgramCapabilities(testCase.template)
			if err != nil {
				t.Fatal(err)
			}
			if actual != testCase.want {
				t.Fatalf("capabilities = %+v, want %+v", actual, testCase.want)
			}
		})
	}
}
