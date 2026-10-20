package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	sqlxtypes "github.com/viant/sqlx/types"
)

type queueNativeScalarRow struct {
	ID           int                `sqlx:"id,primaryKey"`
	At           time.Time          `sqlx:"at_time"`
	Optional     *time.Time         `sqlx:"optional_time"`
	Flag         sqlxtypes.BitBool  `sqlx:"flag"`
	OptionalFlag *sqlxtypes.BitBool `sqlx:"optional_flag"`
	Text         sql.NullString     `sqlx:"text_value"`
	N64          sql.NullInt64      `sqlx:"n64"`
	N32          sql.NullInt32      `sqlx:"n32"`
	N16          sql.NullInt16      `sqlx:"n16"`
	Byte         sql.NullByte       `sqlx:"byte_value"`
	Float        sql.NullFloat64    `sqlx:"float_value"`
	Bool         sql.NullBool       `sqlx:"bool_value"`
	Time         sql.NullTime       `sqlx:"null_time"`
}

func queueNativeScalarFixture(t *testing.T) (*Data, *sql.DB) {
	t.Helper()
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(context.Background(), `CREATE TABLE native_scalars(id INTEGER PRIMARY KEY,at_time DATETIME,optional_time DATETIME,flag BLOB,optional_flag BLOB,text_value TEXT,n64 INTEGER,n32 INTEGER,n16 INTEGER,byte_value INTEGER,float_value REAL,bool_value BOOLEAN,null_time DATETIME)`))
	d := NewData(h.DB)
	require.NoError(t, d.BeginInvocation())
	return d, h.DB
}
func queueNativeScalarInput(t *testing.T, d *Data, contract rhandler.QueueContract, row *queueNativeScalarRow) {
	t.Helper()
	var payload any = row
	if contract == rhandler.SourceSlice {
		payload = []*queueNativeScalarRow{row}
	}
	require.NoError(t, d.InsertWithQueueContract("native_scalars", payload, contract))
}
func TestQueueContractNativeScalarsSQLite(t *testing.T) {
	for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
		t.Run(fmt.Sprint(contract), func(t *testing.T) {
			d, db := queueNativeScalarFixture(t)
			at := time.Date(2026, 10, 19, 8, 9, 10, 123000000, time.FixedZone("native", 3600))
			flag := sqlxtypes.BitBool(true)
			row := &queueNativeScalarRow{ID: 1, At: at, Optional: &at, Flag: flag, OptionalFlag: &flag, Text: sql.NullString{String: "value", Valid: true}, N64: sql.NullInt64{Int64: 64, Valid: true}, N32: sql.NullInt32{Int32: 32, Valid: true}, N16: sql.NullInt16{Int16: 16, Valid: true}, Byte: sql.NullByte{Byte: 7, Valid: true}, Float: sql.NullFloat64{Float64: 1.25, Valid: true}, Bool: sql.NullBool{Bool: false, Valid: true}, Time: sql.NullTime{Time: at, Valid: true}}
			queueNativeScalarInput(t, d, contract, row)
			queueNativeScalarInput(t, d, contract, &queueNativeScalarRow{ID: 2, At: at, Flag: sqlxtypes.BitBool(false)})
			require.NoError(t, d.Complete(context.Background(), nil))
			var actualTime, optional, nullTime time.Time
			var actualFlag, optionalFlag []byte
			var text string
			var n64, n32, n16, byteValue int
			var floatValue float64
			var boolValue bool
			require.NoError(t, db.QueryRow("SELECT at_time,optional_time,flag,optional_flag,text_value,n64,n32,n16,byte_value,float_value,bool_value,null_time FROM native_scalars WHERE id=1").Scan(&actualTime, &optional, &actualFlag, &optionalFlag, &text, &n64, &n32, &n16, &byteValue, &floatValue, &boolValue, &nullTime))
			require.True(t, at.Equal(actualTime))
			require.True(t, at.Equal(optional))
			require.True(t, at.Equal(nullTime))
			require.Equal(t, []byte{1}, actualFlag)
			require.Equal(t, []byte{1}, optionalFlag)
			require.Equal(t, "value", text)
			require.Equal(t, []int{64, 32, 16, 7}, []int{n64, n32, n16, byteValue})
			require.Equal(t, 1.25, floatValue)
			require.False(t, boolValue)
			var falseFlag []byte
			var nulls int
			require.NoError(t, db.QueryRow("SELECT flag,(optional_time IS NULL)+(optional_flag IS NULL)+(text_value IS NULL)+(n64 IS NULL)+(n32 IS NULL)+(n16 IS NULL)+(byte_value IS NULL)+(float_value IS NULL)+(bool_value IS NULL)+(null_time IS NULL) FROM native_scalars WHERE id=2").Scan(&falseFlag, &nulls))
			require.Equal(t, []byte{0}, falseFlag)
			require.Equal(t, 10, nulls)
		})
	}
}

func TestQueueContractNativeScalarDriftSQLite(t *testing.T) {
	for _, contract := range []rhandler.QueueContract{rhandler.SourceRow, rhandler.SourceSlice} {
		for _, mode := range []string{"time pointee", "time pointer replacement", "location identity", "monotonic representation", "bit pointee", "bit pointer replacement", "null valid", "float bits"} {
			t.Run(fmt.Sprint(contract)+"/"+mode, func(t *testing.T) {
				d, db := queueNativeScalarFixture(t)
				at := time.Now()
				flag := sqlxtypes.BitBool(true)
				row := &queueNativeScalarRow{ID: 1, At: at, Optional: &at, OptionalFlag: &flag, Float: sql.NullFloat64{Float64: 0, Valid: true}}
				queueNativeScalarInput(t, d, contract, row)
				var stored *queueNativeScalarRow
				if contract == rhandler.SourceSlice {
					stored = d.queue[0].data.([]*queueNativeScalarRow)[0]
				} else {
					stored = d.queue[0].data.(*queueNativeScalarRow)
				}
				switch mode {
				case "time pointee":
					at = at.Add(time.Second)
				case "time pointer replacement":
					copy := at
					stored.Optional = &copy
				case "location identity":
					at = at.In(time.FixedZone("different", 0))
				case "monotonic representation":
					at = at.Round(0)
				case "bit pointee":
					flag = false
				case "bit pointer replacement":
					copy := flag
					stored.OptionalFlag = &copy
				case "null valid":
					stored.Text.Valid = true
				case "float bits":
					stored.Float.Float64 = math.Copysign(0, -1)
				}
				require.Error(t, d.Complete(context.Background(), nil), "native scalar drift reached execution")
				var count int
				require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM native_scalars").Scan(&count))
				require.Zero(t, count)
			})
		}
	}
}

func TestQueueNativeTimeImageRetainsRepresentation(t *testing.T) {
	original := time.Now()
	value := &original
	image, err := captureQueueValue(reflect.ValueOf(value), 0)
	require.NoError(t, err)
	require.True(t, image.equal(reflect.ValueOf(value), 0))
	stripped := original.Round(0)
	require.True(t, original.Equal(stripped))
	original = stripped
	require.False(t, image.equal(reflect.ValueOf(value), 0), "monotonic representation was lost")
	for _, v := range []any{sql.NullFloat64{Float64: math.Float64frombits(0x7ff8000000000001), Valid: true}, sql.NullFloat64{Float64: math.Copysign(0, -1)}, (*time.Time)(nil), (*sqlxtypes.BitBool)(nil)} {
		image, err := captureQueueValue(reflect.ValueOf(v), 0)
		require.NoError(t, err)
		require.True(t, image.equal(reflect.ValueOf(v), 0), "stable scalar image differs")
	}
	sameInstant := time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("same", 0))
	image, err = captureQueueValue(reflect.ValueOf(sameInstant), 0)
	require.NoError(t, err)
	other := sameInstant.In(time.FixedZone("same", 0))
	require.True(t, sameInstant.Equal(other))
	require.False(t, image.equal(reflect.ValueOf(other), 0), "location pointer representation was lost")
}

type queueCustomNativeTime struct {
	time.Time
	calls *int
}

func (v queueCustomNativeTime) MarshalJSON() ([]byte, error) {
	*v.calls++
	return []byte(`"custom"`), nil
}

type queueCustomNativeNull struct {
	sql.NullString
	calls *int
}

func (v queueCustomNativeNull) Value() (driver.Value, error) { *v.calls++; return "custom", nil }

type queueCustomNativeText struct{ calls *int }

func (v queueCustomNativeText) MarshalText() ([]byte, error) {
	*v.calls++
	return []byte("custom"), nil
}

type queueCustomNativeBit struct {
	sqlxtypes.BitBool
	calls *int
}

func (v queueCustomNativeBit) Value() (driver.Value, error) { *v.calls++; return []byte{1}, nil }

func TestQueueNativeScalarExactIdentityAndNoCallbacks(t *testing.T) {
	calls := 0
	for _, value := range []any{
		queueCustomNativeTime{Time: time.Now(), calls: &calls},
		&queueCustomNativeTime{Time: time.Now(), calls: &calls},
		queueCustomNativeNull{calls: &calls},
		queueCustomNativeText{calls: &calls},
		queueCustomNativeBit{calls: &calls},
		sql.Null[int64]{V: 17, Valid: true},
	} {
		for _, csv := range []bool{false, true} {
			_, err := captureQueueValueForEncoding(reflect.ValueOf(value), 0, csv)
			require.Error(t, err, "unknown callback-bearing wrapper or generic Null was admitted")
			require.Zero(t, calls, "evidence invoked a callback")
		}
	}
	for _, value := range []any{&sql.NullString{String: "value", Valid: true}, (*sql.NullString)(nil), &sql.NullTime{Time: time.Now(), Valid: true}, (*sql.NullTime)(nil)} {
		image, err := captureQueueValue(reflect.ValueOf(value), 0)
		require.NoError(t, err)
		require.True(t, image.equal(reflect.ValueOf(value), 0))
	}
	value := sql.NullFloat64{Float64: math.Float64frombits(0x7ff8000000000001), Valid: true}
	image, err := captureQueueValue(reflect.ValueOf(&value), 0)
	require.NoError(t, err)
	require.True(t, image.equal(reflect.ValueOf(&value), 0), "stable NaN payload changed")
	value.Float64 = math.Float64frombits(0x7ff8000000000002)
	require.False(t, image.equal(reflect.ValueOf(&value), 0), "distinct NaN bit payload was lost")
}

func TestQueueNativeScalarTypedInterfaces(t *testing.T) {
	var valuer driver.Valuer = sqlxtypes.BitBool(true)
	image, err := captureQueueValue(reflect.ValueOf(&valuer).Elem(), 0)
	require.NoError(t, err)
	require.True(t, image.equal(reflect.ValueOf(&valuer).Elem(), 0))
	valuer = sqlxtypes.BitBool(false)
	require.False(t, image.equal(reflect.ValueOf(&valuer).Elem(), 0), "dynamic native bool drift was lost")
	nullable := &sql.NullString{String: "value", Valid: true}
	valuer = nullable
	image, err = captureQueueValue(reflect.ValueOf(&valuer).Elem(), 0)
	require.NoError(t, err)
	nullable.String = "changed"
	require.False(t, image.equal(reflect.ValueOf(&valuer).Elem(), 0), "interface pointee drift was lost")
	nullable.String = "value"
	valuer = &sql.NullString{String: "value", Valid: true}
	require.False(t, image.equal(reflect.ValueOf(&valuer).Elem(), 0), "interface pointee identity replacement was lost")
	valuer = (*sql.NullString)(nil)
	image, err = captureQueueValue(reflect.ValueOf(&valuer).Elem(), 0)
	require.NoError(t, err)
	require.True(t, image.equal(reflect.ValueOf(&valuer).Elem(), 0))
	var marshaler json.Marshaler = time.Now()
	image, err = captureQueueValue(reflect.ValueOf(&marshaler).Elem(), 0)
	require.NoError(t, err)
	require.True(t, image.equal(reflect.ValueOf(&marshaler).Elem(), 0))
	original := marshaler.(time.Time)
	marshaler = original.Round(0)
	require.False(t, image.equal(reflect.ValueOf(&marshaler).Elem(), 0), "typed JSON interface lost exact time representation")
	marshaler = &original
	image, err = captureQueueValue(reflect.ValueOf(&marshaler).Elem(), 0)
	require.NoError(t, err)
	original = original.Add(time.Second)
	require.False(t, image.equal(reflect.ValueOf(&marshaler).Elem(), 0))
	calls := 0
	valuer = queueCustomNativeNull{calls: &calls}
	_, err = captureQueueValue(reflect.ValueOf(&valuer).Elem(), 0)
	require.Error(t, err)
	require.Zero(t, calls)
	marshaler = queueCustomNativeTime{calls: &calls}
	_, err = captureQueueValue(reflect.ValueOf(&marshaler).Elem(), 0)
	require.Error(t, err)
	require.Zero(t, calls)
	valuer = nil
	_, err = captureQueueValue(reflect.ValueOf(&valuer).Elem(), 0)
	require.Error(t, err, "nil callback-interface behavior changed")
	marshaler = nil
	_, err = captureQueueValue(reflect.ValueOf(&marshaler).Elem(), 0)
	require.Error(t, err, "nil callback-interface behavior changed")
}

func TestQueueNativeScalarCSVImages(t *testing.T) {
	at := time.Now()
	for _, value := range []any{at, &at, sqlxtypes.BitBool(true), sql.NullString{String: "native", Valid: true}} {
		image, err := captureQueueValueForEncoding(reflect.ValueOf(value), 0, true)
		require.NoError(t, err)
		require.True(t, image.equal(reflect.ValueOf(value), 0))
	}
	var text fmt.Stringer = at
	image, err := captureQueueValueForEncoding(reflect.ValueOf(&text).Elem(), 0, true)
	require.NoError(t, err)
	require.True(t, image.equal(reflect.ValueOf(&text).Elem(), 0))
}
