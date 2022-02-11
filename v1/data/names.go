package data

type Names []string

func (c Names) Index() map[string]bool {
	result := make(map[string]bool)
	for _, column := range c {
		keys := KeysOf(column, true)
		for _, key := range keys {
			result[key] = true
		}
	}
	return result
}
