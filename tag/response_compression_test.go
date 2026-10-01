package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestResponseCompressionTagRoundTrip(t *testing.T) {
	source := &spec.Settings{ResponseCompression: &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 2048}}
	settings := SettingsFromSpec(source)
	source.ResponseCompression.MinSizeBytes = 0
	raw, err := settings.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	var actual spec.Settings
	decoded.Apply(&actual)
	decoded.ResponseCompression.MinSizeBytes = 3
	if actual.ResponseCompression.MinSizeBytes != 2048 {
		t.Fatal("generated compression policy was lost or shared")
	}
	for _, s := range []reflect.StructTag{`responseCompression:"bad"`, `responseCompression:"{\"encoding\":\"gzip\",\"minSizeBytes\":-1}"`, `responseCompression:"null"`} {
		if _, err := ParseSettings(s); err == nil {
			t.Fatalf("invalid tag accepted: %s", s)
		}
	}
}
