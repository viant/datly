package session

import (
	"reflect"
	"testing"

	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
)

func TestSessionEnsureValidValue_Transitions(t *testing.T) {
	type T struct {
		A *int
		B *int
	}

	inlineStructSwapped := reflect.StructOf([]reflect.StructField{
		// Deliberately swap field order vs T to ensure the types are not convertible.
		{Name: "B", Type: reflect.TypeOf((*int)(nil))},
		{Name: "A", Type: reflect.TypeOf((*int)(nil))},
	})
	inlinePtrType := reflect.PtrTo(inlineStructSwapped)

	newSelector := func(t *testing.T, paramType reflect.Type) *structology.Selector {
		t.Helper()
		stateStruct := reflect.StructOf([]reflect.StructField{
			{Name: "Param", Type: paramType},
		})
		stateType := structology.NewStateType(stateStruct)
		selector := stateType.Lookup("Param")
		if selector == nil {
			t.Fatalf("failed to lookup selector Param")
		}
		return selector
	}

	intPtrType := reflect.TypeOf((*int)(nil))

	ttPtrType := reflect.TypeOf((*T)(nil))
	sliceOfTTPtrType := reflect.SliceOf(ttPtrType)
	ptrToSliceOfTTPtrType := reflect.PtrTo(sliceOfTTPtrType)
	intType := reflect.TypeOf(int(0))
	sliceOfIntType := reflect.SliceOf(intType)
	ttType := reflect.TypeOf(T{})

	boolPtr := func(v bool) *bool { return &v }

	cases := []struct {
		name         string
		schemaType   reflect.Type
		selectorType reflect.Type
		required     *bool
		value        interface{}
		wantType     reflect.Type
		wantErr      bool
		check        func(t *testing.T, got interface{})
	}{
		{
			name:         "nil-value_ptr-schema_returns-typed-nil",
			schemaType:   intPtrType,
			selectorType: intPtrType,
			value:        nil,
			wantType:     intPtrType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				if !reflect.ValueOf(got).IsNil() {
					t.Fatalf("expected nil pointer, got %v", got)
				}
			},
		},
		{
			name:         "nil-value_slice-schema_returns-nil-slice",
			schemaType:   sliceOfIntType,
			selectorType: sliceOfIntType,
			value:        nil,
			wantType:     sliceOfIntType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				if !reflect.ValueOf(got).IsNil() {
					t.Fatalf("expected nil slice, got %v", got)
				}
			},
		},
		{
			name:         "ptr-struct_to_ptr-to-slice-wraps-single",
			schemaType:   sliceOfTTPtrType,
			selectorType: ptrToSliceOfTTPtrType,
			value: func() interface{} {
				a := 10
				b := 20
				return &T{A: &a, B: &b}
			}(),
			wantType: ptrToSliceOfTTPtrType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				gotSlicePtr := reflect.ValueOf(got)
				if gotSlicePtr.IsNil() {
					t.Fatalf("expected non-nil pointer to slice")
				}
				gotSlice := gotSlicePtr.Elem()
				if gotSlice.Len() != 1 {
					t.Fatalf("expected len=1, got %d", gotSlice.Len())
				}
				if gotSlice.Index(0).IsNil() {
					t.Fatalf("expected element 0 to be non-nil")
				}
			},
		},
		{
			name:         "ptr-struct-nil_to_ptr-to-slice-wraps-empty",
			schemaType:   sliceOfTTPtrType,
			selectorType: ptrToSliceOfTTPtrType,
			value:        (*T)(nil),
			wantType:     ptrToSliceOfTTPtrType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				gotSlicePtr := reflect.ValueOf(got)
				if gotSlicePtr.IsNil() {
					t.Fatalf("expected non-nil pointer to slice")
				}
				gotSlice := gotSlicePtr.Elem()
				if gotSlice.Len() != 0 {
					t.Fatalf("expected len=0, got %d", gotSlice.Len())
				}
			},
		},
		{
			name:         "slice-to-scalar_len0_required_errors",
			schemaType:   ttPtrType,
			selectorType: ttPtrType,
			required:     boolPtr(true),
			value:        []*T{},
			wantErr:      true,
		},
		{
			name:         "slice-to-scalar_len0_not-required_returns-zero",
			schemaType:   ttPtrType,
			selectorType: ttPtrType,
			value:        []*T{},
			wantType:     ttPtrType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				if reflect.ValueOf(got).IsNil() {
					t.Fatalf("expected non-nil *T")
				}
			},
		},
		{
			name:         "slice-of-int_len1_to-int",
			schemaType:   intType,
			selectorType: intType,
			value:        []int{7},
			wantType:     intType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				if got.(int) != 7 {
					t.Fatalf("expected 7, got %v", got)
				}
			},
		},
		{
			name:         "slice-of-int_len2_to-int_errors",
			schemaType:   intType,
			selectorType: intType,
			value:        []int{1, 2},
			wantErr:      true,
		},
		{
			name:         "ptr-required_nil_errors",
			schemaType:   ttPtrType,
			selectorType: ttPtrType,
			required:     boolPtr(true),
			value:        (*T)(nil),
			wantErr:      true,
		},
		{
			name:         "ptr-value_to-struct-selector_derefs",
			schemaType:   ttType,
			selectorType: ttType,
			value: func() interface{} {
				a := 3
				b := 4
				return &T{A: &a, B: &b}
			}(),
			wantType: ttType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				gotT := got.(T)
				if gotT.A == nil || gotT.B == nil {
					t.Fatalf("expected non-nil fields")
				}
				if *gotT.A != 3 || *gotT.B != 4 {
					t.Fatalf("unexpected values: %+v", gotT)
				}
			},
		},
		{
			name:         "struct-value_to-ptr-selector_allocates",
			schemaType:   ttPtrType,
			selectorType: ttPtrType,
			value: func() interface{} {
				a := 5
				b := 6
				return T{A: &a, B: &b}
			}(),
			wantType: ttPtrType,
			check: func(t *testing.T, got interface{}) {
				t.Helper()
				gotPtr := got.(*T)
				if gotPtr == nil || gotPtr.A == nil || gotPtr.B == nil {
					t.Fatalf("expected non-nil *T with non-nil fields")
				}
				if *gotPtr.A != 5 || *gotPtr.B != 6 {
					t.Fatalf("unexpected values: %+v", *gotPtr)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parameter := &state.Parameter{
				Name:     "Param",
				In:       state.NewState("Param"),
				Schema:   state.NewSchema(tc.schemaType),
				Required: tc.required,
			}

			selector := newSelector(t, tc.selectorType)
			sess := &Session{}
			opts := NewOptions(WithReportNotAssignable(false))

			got, err := sess.ensureValidValue(tc.value, parameter, selector, opts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if tc.wantType != nil && reflect.TypeOf(got) != tc.wantType {
				t.Fatalf("expected %v, got %T", tc.wantType, got)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}

	t.Run("slice-of-named-ptr_to-inline-ptr_allocates-and-copies_details", func(t *testing.T) {
		a := 1
		b := 2
		original := &T{A: &a, B: &b}
		input := []*T{original}

		parameter := &state.Parameter{
			Name:   "Param",
			In:     state.NewState("Param"),
			Schema: state.NewSchema(inlinePtrType),
		}
		selector := newSelector(t, inlinePtrType)
		sess := &Session{}
		opts := NewOptions(WithReportNotAssignable(false))

		got, err := sess.ensureValidValue(input, parameter, selector, opts)
		if err != nil {
			t.Fatalf("ensureValidValue error: %v", err)
		}
		if reflect.TypeOf(got) != inlinePtrType {
			t.Fatalf("expected %v, got %T", inlinePtrType, got)
		}

		gotPtr := reflect.ValueOf(got).Pointer()
		origPtr := reflect.ValueOf(original).Pointer()
		if gotPtr == origPtr {
			t.Fatalf("expected ensureValidValue to allocate/copy into %v; got aliases original *T pointer %x", inlinePtrType, gotPtr)
		}

		gotValue := reflect.ValueOf(got).Elem()
		gotA := gotValue.FieldByName("A")
		gotB := gotValue.FieldByName("B")
		if gotA.IsNil() || gotB.IsNil() {
			t.Fatalf("expected A and B to be non-nil")
		}
		if gotA.Elem().Int() != int64(*original.A) {
			t.Fatalf("expected A=%d, got %d", *original.A, gotA.Elem().Int())
		}
		if gotB.Elem().Int() != int64(*original.B) {
			t.Fatalf("expected B=%d, got %d", *original.B, gotB.Elem().Int())
		}
	})
}
