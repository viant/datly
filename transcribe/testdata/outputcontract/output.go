package outputcontract

import (
	"context"
	"encoding/json"
	"github.com/viant/xdatly/response"
)

type Row struct {
	ID int `sqlx:"id"`
}

type Output struct {
	Data              *Row `parameter:"Data,kind=output,in=view" view:"Records" sql:"uri=sql/records.sql" json:"-"`
	response.Response `json:"-"`
}

func (o *Output) Finalize(context.Context) error {
	var value any
	if o.Data != nil && o.Data.ID != 0 {
		value = map[string]int{"LEGACY_ID": o.Data.ID}
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	o.Response = response.NewBuffered(response.WithStatusCode(200), response.WithHeader("Content-Type", "application/json"), response.WithBytes(body))
	return nil
}

// Invalid contracts exercise authoring rejection before file emission.
type MissingBinding struct{ Data *Row }
type WrongBinding struct {
	Data *Row `parameter:"Data,kind=output,in=body"`
}
