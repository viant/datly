package reader

func dereferencer(i interface{}) func(interface{}) interface{} {
	switch i.(type) {
	case *int64:
		return func(value interface{}) interface{} {
			return int(*value.(*int64))
		}

	case *uint64:
		return func(value interface{}) interface{} {
			return int(*value.(*uint64))
		}

	case *float32:
		return func(value interface{}) interface{} {
			return float64(*value.(*float32))
		}

	case *string:
		return func(value interface{}) interface{} {
			return *value.(*string)
		}
	case *bool:
		return func(value interface{}) interface{} {
			return *value.(*bool)
		}
	default:
		return func(i interface{}) interface{} {
			return i
		}
	}
}
