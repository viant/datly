package _48_custom_unmarshall_velty

var PackageName = "preference"

type Preference struct {
	Id        int  `sqlx:"ID,autoincrement,primaryKey,required"`
	Object    *Foo `sqlx:"OBJECT,enc=JSON" json:",omitempty" `
	ClassName string
	Has       *PreferenceHas `setMarker:"true" typeName:"PreferenceHas" json:"-" sqlx:"-"`
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
