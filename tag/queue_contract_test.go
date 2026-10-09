package tag

import "testing"

func TestQueueContractViewTagRoundTrip(t *testing.T) {
	for _, contract := range []string{"source-row", "source-slice"} {
		v, err := (View{Name: "Rows", QueueContract: contract}).Value()
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseView(v)
		if err != nil || got.QueueContract != contract {
			t.Fatal(got, err)
		}
	}
	for _, value := range []string{"Rows,queueContract=unknown", "Rows,queueContract=source-row,queueContract=source-row", "Rows,queueContract=source-slice,queueContract=source-row"} {
		if _, err := ParseView(value); err == nil {
			t.Fatal(value)
		}
	}
	if _, err := (View{Name: "Rows", QueueContract: "unknown"}).Value(); err == nil {
		t.Fatal("unknown contract admitted")
	}
}
