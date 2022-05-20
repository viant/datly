package view

func normalizeKey(key interface{}) interface{} {
	switch actual := key.(type) {
	case *int64:
		if actual == nil {
			return nil
		}
		return int(*actual)
	case *int32:
		if actual == nil {
			return nil
		}
		return int(*actual)

	case *int16:
		if actual == nil {
			return nil
		}
		return int(*actual)

	case int32:
		return int(actual)
	case int64:
		return int(actual)
	case int16:
		return int(actual)
	case *int:
		if actual == nil {
			return nil
		}
		return int(*actual)
	}
	return key
}
