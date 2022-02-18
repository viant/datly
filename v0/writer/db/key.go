package db

import (
	"github.com/viant/datly/v0/data"
	"github.com/viant/dsc"
	"github.com/viant/gtly"
)

//keySetter represents id/key mutator.
type keySetter struct {
	view *data.View
}

//SetKey sets autoincrement/sql value to the application domain instance.
func (s keySetter) SetKey(instance interface{}, seq int64) {
	obj := instance.(gtly.Object)
	obj.SetValue(s.view.PrimaryKey[0], seq)
}

//NewKeySetter creates a key setter
func NewKeySetter(view *data.View) dsc.KeySetter {
	return &keySetter{view: view}
}
