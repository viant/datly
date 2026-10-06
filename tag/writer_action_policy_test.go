package tag

import "testing"

func TestWriterActionPolicyRoundTrip(t *testing.T) {
	value, err := (View{Name: "Rows", WriterActionPolicy: "insert-delete"}).Value()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseView(value)
	if err != nil || got.WriterActionPolicy != "insert-delete" {
		t.Fatalf("policy lost: %+v %v", got, err)
	}
	for _, v := range []string{"Rows,writerActionPolicy=unknown", "Rows,writerActionPolicy=insert-delete,writerActionPolicy=insert-delete"} {
		if _, err := ParseView(v); err == nil {
			t.Fatalf("invalid policy accepted: %s", v)
		}
	}
	if _, err := (View{Name: "Rows", WriterActionPolicy: "unknown"}).Value(); err == nil {
		t.Fatal("unknown policy formatted")
	}
}
