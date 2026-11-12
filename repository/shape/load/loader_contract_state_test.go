package load

import (
	"reflect"
	"testing"
)

func TestContractStates_PreservesCodecAndHandler(t *testing.T) {
	type input struct {
		Jwt string `parameter:",kind=header,in=Authorization,errorCode=401" codec:"JwtClaim"`
		Run string `parameter:",kind=body,in=run" handler:"Exec"`
	}
	states := contractStates(reflect.TypeOf(input{}))
	if got, want := len(states), 2; got != want {
		t.Fatalf("expected %d states, got %d", want, got)
	}
	if states[0].Output == nil || states[0].Output.Name != "JwtClaim" {
		t.Fatalf("expected codec to be preserved, got %#v", states[0].Output)
	}
	if states[1].Handler == nil || states[1].Handler.Name != "Exec" {
		t.Fatalf("expected handler to be preserved, got %#v", states[1].Handler)
	}
}
