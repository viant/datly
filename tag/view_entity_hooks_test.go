package tag

import (
	"strings"
	"testing"
)

func TestViewEntityHooksRoundTrip(t *testing.T) {
	for _, reference := range []string{"app.Hooks", "example.com/private/application.Hooks", "Hooks"} {
		t.Run(reference, func(t *testing.T) {
			encoded, err := (View{Name: "Rows", EntityHooks: reference}).Value()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := ParseView(encoded)
			if err != nil || decoded.EntityHooks != reference {
				t.Fatalf("encoded=%q decoded=%+v error=%v", encoded, decoded, err)
			}
		})
	}
}

func TestViewEntityHooksErrors(t *testing.T) {
	for _, value := range []string{"Rows,entityHooks=", `Rows,entityHooks=" "`, "Rows,entityHooks=a.Hooks,entityHooks=b.Hooks", "Rows,entityHooks=a.Hooks,entityHooks=a.Hooks", "Rows,entityHook=a.Hooks"} {
		t.Run(value, func(t *testing.T) {
			_, err := ParseView(value)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "entityhook") {
				t.Fatalf("ParseView(%q)=%v", value, err)
			}
		})
	}
}
