package session

import (
	"fmt"
	"strconv"
)

func equals(x interface{}, value string) (bool, error) {
	switch actual := x.(type) {
	case string:
		return actual == value, nil
	case []string:
		for _, candidate := range actual {
			if candidate == value {
				return true, nil
			}
		}
		return false, nil
	case []interface{}:
		for _, candidate := range actual {
			if candidate == value {
				return true, nil
			}
		}
		return false, nil
	case int:
		intY, err := strconv.Atoi(value)
		if err != nil {
			return false, nil
		}
		return actual == intY, nil
	default:
		return false, fmt.Errorf("type not yet supported: %T", x)
	}
	return false, nil
}
