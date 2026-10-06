package spec

import (
	"encoding/json"
	"testing"
)

func TestComponentCallPolicyContract(t *testing.T) {
	for _, policy := range []string{"", "imperative", "buffered"} {
		t.Run(policy, func(t *testing.T) {
			original := &Component{Settings: &Settings{ComponentCallPolicy: policy}}
			if err := original.Settings.ValidateComponentCallPolicy(); err != nil {
				t.Fatal(err)
			}
			if original.Settings.IsZero() != (policy == "") {
				t.Fatalf("IsZero for %q", policy)
			}
			clone := original.Clone()
			original.Settings.ComponentCallPolicy = "changed"
			if clone.Settings.ComponentCallPolicy != policy {
				t.Fatal("clone lost caller policy")
			}
			data, err := json.Marshal(clone)
			if err != nil {
				t.Fatal(err)
			}
			var decoded Component
			if err = json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Settings == nil || decoded.Settings.ComponentCallPolicy != policy {
				t.Fatalf("JSON lost policy: %s", data)
			}
		})
	}
	for _, policy := range []string{"unknown", "BUFFERED", " buffered ", "binding"} {
		if err := (&Settings{ComponentCallPolicy: policy}).ValidateComponentCallPolicy(); err == nil {
			t.Fatalf("accepted unsupported policy %q", policy)
		}
	}
	if err := (&Settings{ComponentCallPolicy: "buffered", IndependentChildTransactions: true}).ValidateComponentCallPolicy(); err == nil {
		t.Fatal("accepted independent buffered calls")
	}
	for _, policy := range []string{"", "imperative"} {
		if err := (&Settings{ComponentCallPolicy: policy, IndependentChildTransactions: true}).ValidateComponentCallPolicy(); err != nil {
			t.Fatal(err)
		}
	}
	var absent *Settings
	if err := absent.ValidateComponentCallPolicy(); err != nil {
		t.Fatal(err)
	}
}
