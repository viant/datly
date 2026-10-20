package tag

import "testing"

func TestWriterUpdateActionPolicyRoundTrip(t *testing.T) {
	value, err := (View{Name: "Rows", WriterActionPolicy: "update"}).Value()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseView(value)
	if err != nil || got.WriterActionPolicy != "update" {
		t.Fatalf("policy: %+v %v", got, err)
	}
}
