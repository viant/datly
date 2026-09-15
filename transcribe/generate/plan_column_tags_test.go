package generate

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestScalarColumnFieldTagMergesSQLXConstraints(t *testing.T) {
	tests := []struct {
		name   string
		column *spec.Column
		want   string
	}{
		{name: "exact output alias", column: &spec.Column{Name: "Alias_Name"}, want: `sqlx:"event_id|Alias_Name"`},
		{name: "authored mapping retains authority", column: &spec.Column{Name: "Alias_Name", Tag: `sqlx:"custom|Alias_Name"`}, want: `sqlx:"custom|Alias_Name"`},
		{name: "table not null", column: &spec.Column{NotNull: true, Nullable: true}, want: `sqlx:"event_id,required=true"`},
		{name: "explicit required false", column: &spec.Column{NotNull: true, Tag: `sqlx:"event_id,required=false"`}, want: `sqlx:"event_id,required=false"`},
		{name: "nullable inference is not a table constraint", column: &spec.Column{Nullable: false}, want: `sqlx:"event_id"`},
		{name: "transient not null", column: &spec.Column{NotNull: true, Tag: `sqlx:"-"`}, want: `sqlx:"-"`},
		{
			name:   "discovered constraints",
			column: &spec.Column{PrimaryKey: true, AutoIncrement: true, Unique: true},
			want:   `sqlx:"event_id,primaryKey=true,autoincrement=true,unique=true"`,
		},
		{
			name:   "authored false options retain authority",
			column: &spec.Column{PrimaryKey: true, Unique: true, Tag: `json:"id" sqlx:"name=event_id,primaryKey=false,unique=false"`},
			want:   `json:"id" sqlx:"name=event_id,primaryKey=false,unique=false"`,
		},
		{
			name:   "authored generator already expresses autoincrement",
			column: &spec.Column{PrimaryKey: true, AutoIncrement: true, Tag: `sqlx:"event_id,generator=autoincrement"`},
			want:   `sqlx:"event_id,generator=autoincrement"`,
		},
		{
			name:   "authored transient mapping remains untouched",
			column: &spec.Column{PrimaryKey: true, AutoIncrement: true, Unique: true, Tag: `json:"-" sqlx:"-"`},
			want:   `json:"-" sqlx:"-"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := scalarColumnFieldTag(test.column, "event_id", false); actual != test.want {
				t.Fatalf("scalarColumnFieldTag() = %q, want %q", actual, test.want)
			}
		})
	}
}

func TestScalarColumnFieldTagAddsVeltyAliasesOnlyForVeltyTarget(t *testing.T) {
	column := &spec.Column{Name: "IS_AUTH", Source: "IS_AUTH", Type: spec.TypeRef{Name: "int"}}
	if actual := scalarColumnFieldTag(column, "IS_AUTH", true); actual != `sqlx:"IS_AUTH" velty:"names=IS_AUTH|IsAuth"` {
		t.Fatalf("Velty column tag = %q", actual)
	}
	if actual := scalarColumnFieldTag(column, "IS_AUTH", false); actual != `sqlx:"IS_AUTH"` {
		t.Fatalf("non-Velty column tag = %q", actual)
	}
	column.Tag = `velty:"names=Auth"`
	if actual := scalarColumnFieldTag(column, "IS_AUTH", true); actual != `velty:"names=Auth" sqlx:"IS_AUTH"` {
		t.Fatalf("authored Velty column tag = %q", actual)
	}
	column.Tag = `sqlx:"-"`
	if actual := scalarColumnFieldTag(column, "IS_AUTH", true); actual != `sqlx:"-"` {
		t.Fatalf("transient column tag = %q", actual)
	}
	column.Tag = `sqlx:"-" velty:"names=Auth"`
	if actual := scalarColumnFieldTag(column, "IS_AUTH", true); actual != `sqlx:"-" velty:"names=Auth"` {
		t.Fatalf("authored transient Velty authority = %q", actual)
	}
}
