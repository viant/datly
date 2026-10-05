package writer

import (
	rhandler "github.com/viant/datly/runtime/handler"
	"testing"
)

var _ rhandler.PreBindingTransaction = (*Handler)(nil)

func TestWriterRequestsTransactionBeforeBindingWritableGraph(t *testing.T) {
	for _, tc := range []struct {
		name string
		root *Record
		want bool
	}{
		{"physical root", &Record{}, true},
		{"auxiliary root with writable child", &Record{Auxiliary: true, Relations: []*Relation{{Child: &Record{}}}}, true},
		{"nested writable child", &Record{Auxiliary: true, Relations: []*Relation{{Child: &Record{Auxiliary: true, Relations: []*Relation{{Child: &Record{}}}}}}}, true},
		{"leaf auxiliary", &Record{Auxiliary: true}, false},
		{"read-only graph", &Record{Auxiliary: true, Relations: []*Relation{{Child: &Record{Auxiliary: true}}}}, false},
		{"missing root", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{metadata: &Metadata{Root: tc.root}, preBindingTransaction: hasWritableRole(tc.root)}
			if got := h.RequiresPreBindingTransaction(); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	var absent *Handler
	if absent.RequiresPreBindingTransaction() || (&Handler{}).RequiresPreBindingTransaction() {
		t.Fatal("unconfigured writer requested transaction")
	}
}
