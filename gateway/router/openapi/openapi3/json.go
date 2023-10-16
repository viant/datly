package openapi3

import (
	"bytes"
)

func mergeJSON(j1, j2 []byte) []byte {
	var result = make([]byte, len(j1)+len(j2))
	copied := copy(result, j1)
	if copied == 2 {
		return j2
	}
	if index := bytes.LastIndex(j1, []byte("}")); index != -1 {
		result[index] = ','
		index++
		copied = copy(result[index:], j2[1:])
		result = result[:index+copied]
	}
	return result
}
