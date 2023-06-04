package generated

type Foos struct {
	Id       int      `sqlx:"name=ID,primaryKey"`
	Name     *string  `sqlx:"name=NAME" json:",omitempty"`
	Quantity *int     `sqlx:"name=QUANTITY" json:",omitempty"`
	Has      *FoosHas `setMarker:"true" typeName:"FoosHas" json:"-" sqlx:"-"`
}

type FoosHas struct {
	Id       bool
	Name     bool
	Quantity bool
}
