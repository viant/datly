package xmlfilter

import (
	"fmt"
	"testing"
	"time"
)

func TestService_Transfer(t *testing.T) {

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
		IntFilter    IntFilter `xmlify:"name=integer_filter"`
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

func TestService_Transfer02(t *testing.T) {

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
		IntFilter    *IntFilter
		StringFilter *StringFilter
		BoolFilter   *BoolFilter
	}

	q := Filters{
		IntFilter: &IntFilter{
			Include: []int{1, 2},
			Exclude: []int{4, 5},
		},
		StringFilter: &StringFilter{
			Include: []string{"a", "b", "c", "d"},
			Exclude: []string{"E", "F", "G", "H"},
		},

		BoolFilter: &BoolFilter{
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

func TestService_Transfer03(t *testing.T) {

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
		IntFilter    *IntFilter
		StringFilter *StringFilter
		BoolFilter   *BoolFilter
	}

	q := Filters{
		IntFilter: &IntFilter{
			Include: []int{1, 2},
			Exclude: nil,
		},
		StringFilter: nil,

		BoolFilter: &BoolFilter{
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

func TestService_BLA1_test(t *testing.T) {
	f := 654.923
	i := int(f)
	fmt.Printf("%d\n ", i)

	var start time.Time
	//start := time.Now()
	time.Sleep(3 * time.Second)

	end := time.Now()
	elapsed := end.Sub(start)
	elapsedSec := int(elapsed.Seconds())
	fmt.Printf("elapsedSec == %d\n", elapsedSec)
}
