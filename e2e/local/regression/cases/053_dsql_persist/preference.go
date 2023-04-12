package _48_custom_unmarshall_velty

var PackageName = "preference"

type Preference struct {
	Id     int            `sqlx:"name=ID,autoincrement,primaryKey,required"`
	Object *Foo           `sqlx:"name=OBJECT" json:",omitempty" jsonx:"inline=true"`
	Has    *PreferenceHas `presenceIndex:"true" typeName:"PreferenceHas" json:"-" sqlx:"presence=true"`
}

type PreferenceHas struct {
	Id        bool
	Object    bool
	ClassName bool
}

type Foo struct {
	Id    int
	Name  string
	Price float64
	Info  map[string]int
}
