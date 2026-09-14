package sequencer

import (
	"fmt"
	"math"

	"github.com/viant/sqlx/metadata/sink"
)

type reservationKey struct{ catalog, schema, name string }

// reserve retains the native sequence's exclusive high-water mark in this
// DB/transaction-bound service. Database MAX does not see unqueued allocations.
// No SQL, identifier policy, or per-emitter counters are introduced here.
func (s *Service) reserve(table string, native *sink.Sequence, count int) (*sink.Sequence, error) {
	if err := s.validateRange(native, count); err != nil {
		return nil, err
	}
	key := reservationKey{native.Catalog, native.Schema, native.Name}
	if key.name == "" {
		key.name = table
	}
	if s.reservations == nil {
		s.reservations = map[reservationKey]sink.Sequence{}
	}
	result := *native
	previous, exists := s.reservations[key]
	if exists && (previous.IncrementBy != result.IncrementBy || previous.StartValue != result.StartValue) {
		return nil, fmt.Errorf("sequence %s changed native increment or start within a transaction", key.name)
	}
	if exists && previous.Value > result.MinValue(int64(count)) {
		result.Value = previous.Value
		if int64(count) > math.MaxInt64/result.IncrementBy || result.Value > math.MaxInt64-int64(count)*result.IncrementBy {
			return nil, fmt.Errorf("sequence %s reservation overflows int64", key.name)
		}
		result.Value = result.NextValue(int64(count))
		if result.Value < previous.Value {
			return nil, fmt.Errorf("sequence %s aligned reservation overflows int64", key.name)
		}
	}
	if err := s.validateRange(&result, count); err != nil {
		return nil, err
	}
	s.reservations[key] = result
	return &result, nil
}

func (s *Service) validateRange(native *sink.Sequence, count int) error {
	if native == nil || native.IncrementBy <= 0 || count <= 0 {
		return fmt.Errorf("native sequence range is invalid")
	}
	if int64(count) > math.MaxInt64/native.IncrementBy {
		return fmt.Errorf("native sequence range width overflows int64")
	}
	// Guard native alignment arithmetic before calling MinValue.
	if native.StartValue < 0 && native.Value > math.MaxInt64+native.StartValue {
		return fmt.Errorf("native sequence alignment overflows int64")
	}
	width := int64(count) * native.IncrementBy
	if native.Value < math.MinInt64+width {
		return fmt.Errorf("native sequence range underflows int64")
	}
	if native.Value > native.StartValue {
		aligned := native.Value - (native.Value-native.StartValue)%native.IncrementBy
		if aligned < math.MinInt64+width {
			return fmt.Errorf("native aligned sequence range underflows int64")
		}
	}
	min := native.MinValue(int64(count))
	if native.Value <= min || min < native.StartValue || min > math.MaxInt64-width || min+width > native.Value {
		return fmt.Errorf("native sequence returned an invalid reservation")
	}
	if native.MaxValue > 0 && min+width-native.IncrementBy > native.MaxValue {
		return fmt.Errorf("native sequence exceeds maximum value")
	}
	return nil
}
