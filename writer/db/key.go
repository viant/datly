package db

import (
	"github.com/viant/datly/data"
	"github.com/viant/gtly"
	"github.com/viant/dsc"
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
