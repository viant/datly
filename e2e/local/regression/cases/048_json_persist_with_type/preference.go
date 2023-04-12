package _48_custom_unmarshall_velty

import (
	"reflect"
)

var PackageName = "preference"

var Types = map[string]reflect.Type{
	"Preference": reflect.TypeOf(Preference{}),
}

type Preference struct {
	Id        int            `sqlx:"name=ID,autoincrement,primaryKey,required"`
	Object    interface{}    `sqlx:"name=OBJECT" json:",omitempty"`
	ClassName string         `sqlx:"name=CLASS_NAME" `
	Has       *PreferenceHas `presenceIndex:"true" typeName:"PreferenceHas" json:"-" sqlx:"presence=true"`
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
