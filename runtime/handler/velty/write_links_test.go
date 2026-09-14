package velty

import (
	"strings"
	"testing"
)

func TestWriteHooksRelationKeyConversions(t *testing.T) {
	type value struct{ ID int64 }
	type pointer struct{ ID *int64 }
	w := &WriteHooks{}
	parent := &value{ID: 7}
	child := &pointer{}
	if _, err := w.Link(child, parent, "ID", "ID"); err != nil {
		t.Fatal(err)
	}
	if child.ID != &parent.ID {
		t.Fatal("address link lost parent identity")
	}
	parent.ID = 9
	if *child.ID != 9 {
		t.Fatal("address link did not retain parent mutation")
	}
	for _, test := range []struct {
		name    string
		parent  any
		want    int64
		failure string
	}{
		{"direct", parent, 9, ""},
		{"dereference", child, 9, ""},
		{"nil dereference", &pointer{}, 0, "missing parent key"},
		{"different numeric type", &struct{ ID int }{7}, 0, "unsupported relation key conversion"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := &value{}
			_, err := w.Link(target, test.parent, "ID", "ID")
			if test.failure != "" {
				if err == nil || !strings.Contains(err.Error(), test.failure) {
					t.Fatalf("error %v", err)
				}
				return
			}
			if err != nil || target.ID != test.want {
				t.Fatalf("target %+v, error %v", target, err)
			}
		})
	}
}
