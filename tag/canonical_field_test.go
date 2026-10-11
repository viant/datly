package tag

import "testing"

func TestCanonicalFieldTagDiscardsOnlyObsoleteMetadata(t *testing.T) {
	supported := `sqlx:"id|user_id,table=users,db=sales" json:"userId,omitempty" codec:"JSON" setMarker:"false"`
	raw := supported + ` docTable:"wrong" docColumn:"wrong"`
	if got := CanonicalFieldTag(raw); got != supported {
		t.Fatalf("supported canonical tags changed: %s", got)
	}
	if got := CanonicalFieldTag(supported); got != supported {
		t.Fatalf("normalization is not idempotent: %s", got)
	}
}
