package reader

func sliceWithLimit(aSlice []interface{}, from, to int) ([]interface{}, int) {
	if len(aSlice) > to {
		return aSlice[from:to], to - from
	}

	return aSlice[from:], len(aSlice) - from
}
