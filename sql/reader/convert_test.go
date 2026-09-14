package reader

import (
	"reflect"
	"testing"

	sqlxio "github.com/viant/sqlx/io"
)

func TestConvertCollectorSlice(t *testing.T) {
	t.Run("invalid source", func(t *testing.T) {
		actual := convertCollectorSlice(reflect.Value{}, reflect.TypeOf([]int{}))
		if actual.Len() != 0 {
			t.Fatalf("expected zero slice, got %+v", actual.Interface())
		}
	})

	t.Run("assignable source", func(t *testing.T) {
		source := reflect.ValueOf([]int{1, 2})
		actual := convertCollectorSlice(source, reflect.TypeOf([]int{}))
		if !reflect.DeepEqual(actual.Interface(), []int{1, 2}) {
			t.Fatalf("unexpected assignable result: %+v", actual.Interface())
		}
	})

	t.Run("non slice target", func(t *testing.T) {
		actual := convertCollectorSlice(reflect.ValueOf([]int{1}), reflect.TypeOf(0))
		if actual.IsValid() {
			t.Fatalf("expected invalid result for non-slice target")
		}
	})

	t.Run("slice to pointer slice", func(t *testing.T) {
		source := reflect.ValueOf([]struct{ ID int }{{ID: 7}})
		targetType := reflect.TypeOf([]*struct{ ID int }{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 1 || actual.Index(0).Elem().FieldByName("ID").Int() != 7 {
			t.Fatalf("unexpected pointer slice conversion: %+v", actual.Interface())
		}
	})

	t.Run("pointer slice to value slice", func(t *testing.T) {
		source := reflect.ValueOf([]*struct{ ID int }{{ID: 9}})
		targetType := reflect.TypeOf([]struct{ ID int }{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 1 || actual.Index(0).FieldByName("ID").Int() != 9 {
			t.Fatalf("unexpected value slice conversion: %+v", actual.Interface())
		}
	})

	t.Run("pointer item assignable to pointer target", func(t *testing.T) {
		type row struct{ ID int }
		type sourceRows []*row
		type targetRows []*row
		source := reflect.ValueOf(sourceRows{{ID: 11}})
		targetType := reflect.TypeOf(targetRows{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 1 || actual.Index(0).Elem().FieldByName("ID").Int() != 11 {
			t.Fatalf("unexpected assignable pointer slice conversion: %+v", actual.Interface())
		}
	})

	t.Run("pointer item convertible to pointer target", func(t *testing.T) {
		type sourceRow struct{ ID int }
		type targetRow sourceRow
		source := reflect.ValueOf([]*sourceRow{{ID: 13}})
		targetType := reflect.TypeOf([]*targetRow{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 1 || actual.Index(0).Elem().FieldByName("ID").Int() != 13 {
			t.Fatalf("unexpected convertible pointer slice conversion: %+v", actual.Interface())
		}
	})

	t.Run("pointer item populates pointer-to-interface target", func(t *testing.T) {
		type row struct{ ID int }
		type targetRows []*interface{}
		source := reflect.ValueOf([]*row{{ID: 15}})
		targetType := reflect.TypeOf(targetRows{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 1 {
			t.Fatalf("expected one converted interface pointer, got %+v", actual.Interface())
		}
		valuePtr, ok := actual.Index(0).Interface().(*interface{})
		if !ok || valuePtr == nil {
			t.Fatalf("expected pointer to interface, got %+v", actual.Index(0).Interface())
		}
		rowValue, ok := (*valuePtr).(row)
		if !ok || rowValue.ID != 15 {
			t.Fatalf("unexpected interface payload: %+v", *valuePtr)
		}
	})

	t.Run("value item convertible to target element", func(t *testing.T) {
		type sourceInt int
		source := reflect.ValueOf([]sourceInt{17})
		targetType := reflect.TypeOf([]int{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 1 || actual.Index(0).Int() != 17 {
			t.Fatalf("unexpected convertible value slice conversion: %+v", actual.Interface())
		}
	})

	t.Run("non convertible item skipped", func(t *testing.T) {
		source := reflect.ValueOf([]string{"abc"})
		targetType := reflect.TypeOf([]int{})
		actual := convertCollectorSlice(source, targetType)
		if actual.Len() != 0 {
			t.Fatalf("expected non convertible item to be skipped, got %+v", actual.Interface())
		}
	})

	t.Run("unmapped resolver nil collector", func(t *testing.T) {
		resolver := unmappedResolver(nil)
		fn := resolver(testColumn{name: "id", scanType: reflect.TypeOf(0)})
		if fn != nil {
			t.Fatalf("expected nil resolver callback for nil collector")
		}
	})
}

func TestConvertCollectorResultSingle(t *testing.T) {
	type row struct{ ID int }

	t.Run("one pointer", func(t *testing.T) {
		actual, err := convertCollectorResult(reflect.ValueOf([]row{{ID: 7}}), reflect.TypeOf((*row)(nil)))
		if err != nil {
			t.Fatalf("convertCollectorResult() error = %v", err)
		}
		if actual.IsNil() || actual.Interface().(*row).ID != 7 {
			t.Fatalf("unexpected single result: %+v", actual.Interface())
		}
	})

	t.Run("empty pointer", func(t *testing.T) {
		actual, err := convertCollectorResult(reflect.ValueOf([]row{}), reflect.TypeOf((*row)(nil)))
		if err != nil {
			t.Fatalf("convertCollectorResult() error = %v", err)
		}
		if !actual.IsNil() {
			t.Fatalf("expected nil single result, got %+v", actual.Interface())
		}
	})

	t.Run("multiple rows", func(t *testing.T) {
		if _, err := convertCollectorResult(reflect.ValueOf([]row{{ID: 1}, {ID: 2}}), reflect.TypeOf((*row)(nil))); err == nil {
			t.Fatal("expected multiple-row error")
		}
	})
}

type testColumn struct {
	name     string
	scanType reflect.Type
}

func (t testColumn) Name() string                      { return t.name }
func (t testColumn) ScanType() reflect.Type            { return t.scanType }
func (t testColumn) Length() (int64, bool)             { return 0, false }
func (t testColumn) DecimalSize() (int64, int64, bool) { return 0, 0, false }
func (t testColumn) Nullable() (bool, bool)            { return false, false }
func (t testColumn) DatabaseTypeName() string          { return "" }
func (t testColumn) Tag() *sqlxio.Tag                  { return nil }

var _ sqlxio.Column = testColumn{}
