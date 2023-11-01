package generated

type Foos struct {
	Id       int      `sqlx:"ID,primaryKey"`
	Name     *string  `sqlx:"NAME" json:",omitempty"`
	Quantity *int     `sqlx:"QUANTITY" json:",omitempty"`
	Has      *FoosHas `setMarker:"true" typeName:"FoosHas" json:"-" sqlx:"-"`
}

type FoosHas struct {
	Id       bool
	Name     bool
	Quantity bool
}
