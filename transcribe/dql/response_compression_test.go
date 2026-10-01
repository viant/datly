package dql

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestResponseCompressionSettingsRoundTrip(t *testing.T) {
	c, err := parseComponentSource("example.com/compression", "Records", `#setting($_ = $route('/records','GET'))
#setting($_ = $response_compression('gzip',2048))
SELECT id FROM records`)
	if err != nil {
		t.Fatal(err)
	}
	want := &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 2048}
	if c.Settings == nil || !reflect.DeepEqual(c.Settings.ResponseCompression, want) {
		t.Fatalf("lost settings: %+v", c.Settings)
	}
	raw, err := dtag.SettingsFromSpec(c.Settings).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := dtag.ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	var got spec.Settings
	decoded.Apply(&got)
	if !reflect.DeepEqual(got.ResponseCompression, want) {
		t.Fatal("transcribed policy did not survive Go tags")
	}
	if (Serializer{}).Export(c, "").Supported {
		t.Fatal("bounded serializer silently dropped compression metadata")
	}
	if _, err := parseComponentSource("example.com/duplicate", "Records", "#setting($_ = $response_compression('gzip',2))\n#setting($_ = $response_compression('gzip',3))\nSELECT id FROM records"); err == nil {
		t.Fatal("duplicate compression directives accepted")
	}

	for _, setting := range []string{`$response_compression('br',2048)`, `$response_compression('gzip',-1)`, `$response_compression('gzip')`, `$response_compression('gzip',1.5)`, `$response_compression('gzip',2).Unknown()`, `$response_compression('gzip',2))\n#setting($_ = $response_compression('gzip',3)`} {
		if _, err := parseComponentSource("example.com/bad", "Records", "#setting($_ = "+setting+")\nSELECT id FROM records"); err == nil {
			t.Fatalf("invalid setting accepted %s", setting)
		}
	}
}
