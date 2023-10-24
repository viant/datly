package tags

import (
	"embed"
	"github.com/viant/afs"
	"github.com/viant/structology/format"
)

type Tag struct {
	embed       *embed.FS
	fs          afs.Service
	View        *View
	SQL         ViewSQL
	SummarySQL  ViewSQLSummary
	Parameter   *Parameter
	LinkOn      LinkOn
	Predicate   *Predicate
	Codec       *Codec
	TypeName    string
	Description *string
	Format      *format.Tag
}

func (t *Tag) EnsurePredicate() *Predicate {
	if t == nil || t.Predicate == nil {
		return &Predicate{}
	}
	return t.Predicate
}

func (t *Tag) ensureView() *View {
	if t.View == nil {
		t.View = &View{}
	}
	return t.View
}
