package xmlfilter

import (
	"fmt"
	"testing"
)

func TestService_Transfer0004(tt *testing.T) {

	type IntFilter struct {
		Include []int
		Exclude []int
	}
	type StringFilter struct {
		Include []string
		Exclude []string
	}
	type BoolFilter struct {
		Include []bool
		Exclude []bool
	}

	type Filters struct {
		IntFilter    IntFilter
		StringFilter StringFilter
		BoolFilter   BoolFilter
	}

	q := Filters{
		IntFilter: IntFilter{
			Include: []int{1, 2},
			Exclude: []int{4, 5},
		},
		StringFilter: StringFilter{
			Include: []string{"a", "b", "c", "d"},
			Exclude: []string{"E", "F", "G", "H"},
		},
		BoolFilter: BoolFilter{
			Include: []bool{true, true},
			Exclude: []bool{false, false},
		},
	}

	srv := New()
	transfer, err := srv.Transfer(q)
	fmt.Println(transfer)
	if transfer != nil {
	}
	if err != nil {
	}

}
