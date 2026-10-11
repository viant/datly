package dql

import "testing"

func TestWithTagRetainsWireTypeDiscoveryMetadata(t *testing.T) {
	component, err := parseComponentSource("example.com/request", "Request", `#setting($_ = $route('/requests', 'POST'))
#define($_ = $Body<Request>(body/).WithTag('wireType:"example.com/request.RequestWire"'))
SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	param := component.Parameters[0]
	if param.WireTypeExpr != "example.com/request.RequestWire" || param.TypeExpr != "Request" || param.Tag != `wireType:"example.com/request.RequestWire"` {
		t.Fatalf("metadata: %+v", param)
	}
	if cloned := component.Clone(); cloned.Parameters[0].WireTypeExpr != param.WireTypeExpr {
		t.Fatal("clone lost metadata")
	}
}
