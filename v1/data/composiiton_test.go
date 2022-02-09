package data

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestAssembleViews(t *testing.T) {
	type Employee struct {
		Id           int
		Name         string
		DepartmentId int
	}

	type Department struct {
		Id      int
		Address string
	}

	type EmployeeAssembled struct {
		Id         int
		Name       string
		Department *Department
	}

	type Foo struct {
		Id   int
		Name string
	}

	type Boo struct {
		Id    string
		FooId int
		Name  string
	}

	type FooBooAssembled struct {
		Id   int
		Name string
		Boos *[]*Boo
	}

	testCases := []struct {
		description string
		main        View
		relations   []*Reference
		result      View
		expectError bool
	}{
		{
			description: "assemble two views with one to one relation",
			main: View{
				Name: "employees",
				Columns: []*Column{
					{
						Name: "id",
					},
					{
						Name: "name",
					},
					{
						Name: "department_id",
					},
				},
				Component: NewComponent(reflect.TypeOf(Employee{})),
			},
			relations: []*Reference{
				{
					Child: &View{
						Name: "departments",
						Columns: []*Column{
							{Name: "id"},
							{Name: "address"},
						},
						Component: NewComponent(reflect.TypeOf(Department{})),
					},
					Cardinality: "One",
					Column:      "departmentId",
					RefColumn:   "id",
					RefHolder:   "department",
				},
			},
			result: View{
				Component: NewComponent(reflect.TypeOf(EmployeeAssembled{})),
			},
		},

		{
			description: "assemble two views with many to one relation",
			main: View{
				Name: "foos",
				Columns: []*Column{
					{
						Name: "id",
					},
					{
						Name: "name",
					},
				},
				Component: NewComponent(reflect.TypeOf(Foo{})),
			},
			relations: []*Reference{
				{
					Child: &View{
						Name: "boos",
						Columns: []*Column{
							{Name: "id"},
							{Name: "fooId"},
							{Name: "name"},
						},
						Component: NewComponent(reflect.TypeOf(Boo{})),
					},
					Name:        "departments",
					Cardinality: "Many",
					Column:      "id",
					RefColumn:   "fooId",
					RefHolder:   "boos",
				},
			},
			result: View{
				Component: NewComponent(reflect.TypeOf(FooBooAssembled{})),
			},
		},
	}

	for _, testCase := range testCases {
		view, err := AssembleView(&testCase.main, testCase.relations...)
		if testCase.expectError {
			assert.NotNil(t, err, testCase.description)
			continue
		}

		assert.True(t, testCase.result.Component.compType.ConvertibleTo(view.Component.compType), testCase.description)
	}
}
