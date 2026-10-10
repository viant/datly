package spec

import "testing"

func TestKeyRoundTrip(t *testing.T) {
	key := Key{
		Kind:  KindComponent,
		Scope: "github.com/acme/app/report",
		Name:  "GET /v1/reports/:id",
	}
	encoded := key.String()
	decoded, err := ParseKey(encoded)
	if err != nil {
		t.Fatalf("ParseKey failed: %v", err)
	}
	if decoded != key {
		t.Fatalf("round trip mismatch: got %#v want %#v", decoded, key)
	}
}

func TestParseKeyRejectsBadShape(t *testing.T) {
	if _, err := ParseKey("component:onlytwo"); err == nil {
		t.Fatalf("expected parse failure")
	}
}

func TestKeyStringCanonicalEscaping(t *testing.T) {
	for _, tc := range []struct {
		key  Key
		want string
	}{
		{Key{Kind: KindComponent, Scope: "example.com/project/pkg", Name: "Audience"}, "component:example.com/project/pkg:Audience"},
		{Key{Kind: KindView, Scope: `project:west\archive`, Name: `row:\name`}, `view:project\:west\\archive:row\:\\name`},
		{Key{Kind: KindComponent, Scope: "", Name: ""}, "component::"},
		{Key{Kind: KindView, Scope: "地域:東京", Name: "値"}, `view:地域\:東京:値`},
	} {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 100; i++ {
				if got := tc.key.String(); got != tc.want {
					t.Fatalf("canonical key changed: got %q want %q", got, tc.want)
				}
			}
		})
	}
}
func BenchmarkKeyStringCanonicalIdentity(b *testing.B) {
	key := Key{Kind: KindComponent, Scope: "example.com/project/pkg/rule/targeting", Name: "AudienceComponent"}
	b.ReportAllocs()
	for b.Loop() {
		if key.String() == "" {
			b.Fatal("missing key")
		}
	}
}
