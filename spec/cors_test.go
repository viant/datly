package spec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCORSResolvePreservesAuthoredPresence(t *testing.T) {
	var authored CORS
	if err := json.Unmarshal([]byte(`{"allowOrigins":[],"allowMethods":[],"allowHeaders":[],"exposeHeaders":[],"allowCredentials":false,"maxAge":0}`), &authored); err != nil {
		t.Fatal(err)
	}
	values := []string{"*"}
	yes := true
	age := int64(60)
	parent := &CORS{AllowOrigins: &values, AllowMethods: &values, AllowHeaders: &values, ExposeHeaders: &values, AllowCredentials: &yes, MaxAge: &age}
	actual := authored.Resolve(parent)
	if !reflect.DeepEqual(actual, &authored) {
		t.Fatalf("explicit values were replaced: %+v", actual)
	}
	inherited := (&CORS{}).Resolve(parent)
	values[0] = "changed"
	yes = false
	age = 0
	if (*inherited.AllowOrigins)[0] != "*" || !*inherited.AllowCredentials || *inherited.MaxAge != 60 {
		t.Fatal("retained mutable global config")
	}
	if (*CORS)(nil).Resolve(nil) != nil {
		t.Fatal("absent config enabled CORS")
	}
	route := (&Route{CORS: inherited}).Clone()
	(*inherited.AllowOrigins)[0] = "changed"
	if (*route.CORS.AllowOrigins)[0] != "*" {
		t.Fatal("route clone aliases CORS")
	}
	encoded, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CORS
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&decoded, &authored) {
		t.Fatal("lost explicit empty/false values on wire")
	}
}
