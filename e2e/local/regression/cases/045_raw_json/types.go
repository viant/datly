package types

import "encoding/json"

type Record struct {
	Id          string
	Preferences *json.RawMessage `jsonx:",inline"`
	ClassName   string
}
