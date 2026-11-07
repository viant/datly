package contract

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	bodyprovider "github.com/viant/bindly/provider/body"
	"github.com/viant/datly/output"
	"github.com/viant/x/shape"
)

func TestCompositeEffectiveJSONRoundTrip(t *testing.T) {
	original := reflect.TypeFor[[]*campaignPolicy]()
	policy, err := New(original, 8)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := policy.Resolve(t.Context(), policyBinder(t, "reader"))
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`[{"Campaign":{"Secret":"blocked","Public":"campaign"},"Spend":4,"AdOrders":[{"AdOrder":{"Secret":"blocked","Public":"order"},"Fees":{"7":[{"Secret":"blocked","Public":"fee"},null]},"Stable":{"Public":"stable"}}]}]`)
	body, err := bodyprovider.New(raw, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, found, err := body.Value(t.Context(), effective, "")
	if err != nil || !found {
		t.Fatalf("body: found=%v err=%v", found, err)
	}
	projection, err := shape.CompileProjection(effective, original)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := projection.Apply(decoded)
	if err != nil {
		t.Fatal(err)
	}
	rows := canonical.([]*campaignPolicy)
	order := rows[0].AdOrders[0]
	fee := order.Fees[7][0]
	if rows[0].Campaign.Secret != "" || order.AdOrder.Secret != "" || fee.Secret != "" {
		t.Fatal("hidden nested values admitted")
	}
	if rows[0].Campaign.Public != "campaign" || order.AdOrder.Public != "order" || fee.Public != "fee" || order.Fees[7][1] != nil {
		t.Fatal("composite shape or values lost")
	}
	// Internal business values must also be hidden when encoding canonical output.
	rows[0].Campaign.Secret = "private"
	order.AdOrder.Secret = "private"
	fee.Secret = "private"
	base, err := (output.Compiler{}).Compile(output.CompileInput{Type: original})
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := base.WithType(effective)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encoder.Encode(t.Context(), "json", canonical)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded.Data), "Secret") || strings.Contains(string(encoded.Data), "private") {
		t.Fatalf("hidden output: %s", encoded.Data)
	}
	var wire []map[string]any
	if err = json.Unmarshal(encoded.Data, &wire); err != nil || len(wire) != 1 {
		t.Fatal(err)
	}
	if fee.Secret != "private" {
		t.Fatal("encoding mutated canonical instance")
	}
}
