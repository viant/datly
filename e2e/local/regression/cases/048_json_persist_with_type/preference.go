package _48_custom_unmarshall_velty

import (
	"reflect"
)

var PackageName = "preference"

var Types = map[string]reflect.Type{
	"Preference": reflect.TypeOf(Preference{}),
}

type Preference struct {
	Id        int            `sqlx:"ID,autoincrement,primaryKey,required"`
	Object    interface{}    `sqlx:"OBJECT" json:",omitempty"`
	ClassName string         `sqlx:"CLASS_NAME" `
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
}
