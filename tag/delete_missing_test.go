package tag

import "testing"

func TestOnDeleteNotFoundRoundTrip(t *testing.T) {
	for _, policy := range []string{"ignore", "error"} {
		v, err := ParseView("rows,onDeleteNotFound=" + policy)
		if err != nil {
			t.Fatal(err)
		}
		text, err := v.Value()
		if err != nil {
			t.Fatal(err)
		}
		copy, err := ParseView(text)
		if err != nil || copy.OnDeleteNotFound != policy {
			t.Fatalf("round trip=%+v error=%v", copy, err)
		}
	}
	for _, value := range []string{"rows,onDeleteNotFound=unknown", "rows,onDeleteNotFound=ignore,onDeleteNotFound=error"} {
		if _, err := ParseView(value); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
	if _, err := (View{OnDeleteNotFound: "unknown"}).Value(); err == nil {
		t.Fatal("formatter accepted unknown policy")
	}
}
