package _48_custom_unmarshall_velty

var PackageName = "preference"

type Preference struct {
	Id     int            `sqlx:"ID,autoincrement,primaryKey,required"`
	Object *Foo           `sqlx:"OBJECT" json:",omitempty"`
	Has    *PreferenceHas `setMarker:"true" typeName:"PreferenceHas" json:"-" `
}

type PreferenceHas struct {
	Id     bool
	Object bool
}

type Foo struct {
	Id    int
	Name  string
	Price float64
	Info  map[string]int
}
