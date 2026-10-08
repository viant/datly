package constant

import "testing"

func TestExpandKnownDefersSecretFieldsWithoutRecursion(t *testing.T) {
	values, err := New(map[string]string{"Host": "mysql.example", "Literal": "$Password"})
	if err != nil {
		t.Fatal(err)
	}
	input := "${Username}:${credentials.Password}@$Host/${Literal}"
	got, err := values.ExpandKnown(input)
	if err != nil || got != "${Username}:${credentials.Password}@mysql.example/$Password" {
		t.Fatalf("got %q: %v", got, err)
	}
	if _, err = values.Path("${Username}"); err == nil {
		t.Fatal("strict path accepted unknown")
	}
	if _, err = values.ExpandKnown("${Password"); err == nil {
		t.Fatal("malformed reference accepted")
	}
}
