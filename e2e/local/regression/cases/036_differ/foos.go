package generated

type Foos struct {
	Id       int      `sqlx:"name=ID,primaryKey"`
	Name     *string  `sqlx:"name=NAME" json:",omitempty"`
	Quantity *int     `sqlx:"name=QUANTITY" json:",omitempty"`
	Has      *FoosHas `presenceIndex:"true" typeName:"FoosHas" json:"-" sqlx:"presence=true"`
}

type FoosHas struct {
	Id       bool
	Name     bool
	Quantity bool
}
