package column

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestApplyWriterMetadataUsesSQLXTags(t *testing.T) {
	component := &spec.Component{RootView: &spec.View{Name: "Records", Columns: []*spec.Column{
		{Name: "id", Tag: `sqlx:"id,primaryKey"`},
		{Name: "sequence_id", Tag: `sqlx:"sequence_id,autoincrement"`},
		{Name: "code", Tag: `sqlx:"code,unique,required"`},
		{Name: "owner_id", Tag: `sqlx:"owner_id,refTable=owners,refColumn=id"`},
	}}}
	if err := ApplyWriterMetadata(component); err != nil {
		t.Fatal(err)
	}
	columns := component.RootView.Columns
	if !columns[0].PrimaryKey || !columns[1].PrimaryKey || !columns[1].AutoIncrement || !columns[2].Unique || !columns[2].NotNull {
		t.Fatalf("writer SQLX metadata was not promoted: %+v", columns)
	}
	if columns[3].PrimaryKey || columns[3].Tag != `sqlx:"owner_id,refTable=owners,refColumn=id"` {
		t.Fatalf("foreign-key authoring was changed: %+v", columns[3])
	}
}
