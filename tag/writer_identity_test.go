package tag

import "testing"

func TestWriterIdentityRoundTrip(t *testing.T) {
	value, err := (View{Name: "Rows", WriterIdentityPolicy: "assigned-update"}).Value()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseView(value)
	if err != nil || parsed.WriterIdentityPolicy != "assigned-update" {
		t.Fatalf("parsed%+v err%v", parsed, err)
	}
	for _, value := range []string{"Rows,writerIdentity=unknown", "Rows,writerIdentity=assigned-update,writerIdentity=assigned-update"} {
		if _, err := ParseView(value); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}
