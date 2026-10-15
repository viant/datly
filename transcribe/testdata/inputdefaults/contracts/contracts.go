package contracts

import (
	"embed"
	a "github.com/viant/datly/standalone/testdata/eager_overlap_a"
	b "github.com/viant/datly/standalone/testdata/eager_overlap_b"
)

type Row struct {
	ID int `sqlx:"id"`
}
type InputA struct {
	Current []*Row `parameter:"Current,kind=view,in=Current" view:"Current" sql:"uri=query.sql"`
}

func (InputA) EmbedFS() *embed.FS { return a.Input{}.EmbedFS() }

type InputB struct {
	Current []*Row `parameter:"Current,kind=view,in=Current" view:"Current" sql:"uri=query.sql"`
}

func (*InputB) EmbedFS() *embed.FS { return b.Input{}.EmbedFS() }

type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"Rows" sql:"uri=query.sql"`
}
