package tag

import "testing"

func TestQueueContractViewTagRoundTrip(t *testing.T) {
	v, err := (View{Name: "Rows", QueueContract: "source-row"}).Value()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseView(v)
	if err != nil || got.QueueContract != "source-row" {
		t.Fatal(got, err)
	}
	for _, value := range []string{"Rows,queueContract=source-slice", "Rows,queueContract=unknown", "Rows,queueContract=source-row,queueContract=source-row"} {
		if _, err = ParseView(value); err == nil {
			t.Fatal(value)
		}
	}
	if _, err = (View{Name: "Rows", QueueContract: "source-slice"}).Value(); err == nil {
		t.Fatal("source-slice authoring must remain unavailable")
	}
}
