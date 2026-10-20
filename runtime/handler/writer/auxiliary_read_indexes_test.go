package writer

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func (*auxiliaryGraphInput) PrepareReadIndexes(context.Context) error {
	return fmt.Errorf("ambiguous application index: joined read")
}

func TestAuxiliaryComponentDoesNotRequireIndependentReadIndexes(t *testing.T) {
	for _, useCase := range []struct {
		description        string
		componentLifecycle bool
		expectedError      bool
	}{
		{"whole component owns reads", true, false},
		{"ordinary writer retains eager checks", false, true},
	} {
		t.Run(useCase.description, func(t *testing.T) {
			native, err := New(auxiliaryGraphComponent(auxiliaryGraphHookType.Name()), reflect.TypeFor[auxiliaryGraphInput](), reflect.TypeFor[auxiliaryGraphOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			native.metadata.componentLifecycle = useCase.componentLifecycle
			input := &auxiliaryGraphInput{Data: []*auxiliaryGraphRow{{Id: 1}}}
			_, err = native.CaptureInput(t.Context(), input)
			if (err != nil) != useCase.expectedError {
				t.Fatalf("capture error=%v, expected error=%v", err, useCase.expectedError)
			}
			if err := input.PrepareReadIndexes(t.Context()); err == nil {
				t.Fatal("explicit index preparation must remain strict")
			}
		})
	}
}
