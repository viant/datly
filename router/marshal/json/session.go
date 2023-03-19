package json

import "bytes"

type Session struct {
	Filters *Filters
	Options []interface{}
	*bytes.Buffer
}
