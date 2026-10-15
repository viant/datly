package rows

type ReadRow struct {
	ID       int          `sqlx:"ID"`
	Children []*ReadChild `sqlx:"-" on:"ID=ParentID" view:",table=read_children" sql:"SELECT ID, PARENT_ID FROM read_children"`
	Has      *ReadRowHas  `setMarker:"true" json:"-" sqlx:"-"`
}
type ReadRowHas struct{ ID, Children bool }
type ReadChild struct {
	ID       int           `sqlx:"ID"`
	ParentID int           `sqlx:"PARENT_ID"`
	Has      *ReadChildHas `setMarker:"true" json:"-" sqlx:"-"`
}
type ReadChildHas struct{ ID, ParentID bool }
type OtherRead struct{ ID int }
