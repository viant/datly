package kind

import (
	"net/http"
)

type Header struct {
	header http.Header
}

func (q *Header) Names() []string {
	var result = make([]string, 0)
	for k := range q.header {
		result = append(result, k)
	}
	return result
}

func (q *Header) Value(name string) (interface{}, bool, error) {
	value, ok := q.header[name]
	if !ok {
		return nil, false, nil
	}
	if len(value) > 0 {
		return value[0], true, nil
	}
	return "", true, nil
}
