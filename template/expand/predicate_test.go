package expand_test

//func TestPredicate(t *testing.T) {
//	type Foo struct {
//		ID       int
//		Name     string
//		Quantity float64
//	}
//
//	type FooHas struct {
//		ID       bool
//		Name     bool
//		Quantity bool
//	}
//
//	testCases := []struct {
//		state  interface{}
//		has    interface{}
//		config []*expand.PredicateConfig
//	}{
//		{
//			state: &Foo{
//				ID:   15,
//				Name: "abc",
//			},
//			has: &FooHas{
//				ID:   true,
//				Name: true,
//			},
//			config: []*expand.PredicateConfig{
//				{
//					Group:       0,
//					StateAccessor: types.NewAccessor(xunsafe.FieldByName(reflect.TypeOf(Foo{}), "ID")),
//					HasAccessor:   types.NewAccessor(xunsafe.FieldByName(reflect.TypeOf(FooHas{}), "ID")),
//					Expander: func(ctx *expand.Group, value interface{}) (*parameter.Criteria, error) {
//						return &parameter.Criteria{
//							Query: "ID = ?",
//							Args:  []interface{}{value},
//						}, nil
//					},
//				},
//			},
//		},
//	}
//
//	for _, testCase := range testCases {
//		predicate := expand.NewPredicate(nil, testCase.state, testCase.has, testCase.config)
//
//		result, err := predicate.Expand(0)
//		fmt.Print(result, err)
//	}
//}
