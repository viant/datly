package sequencer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWalker_CountEmpty(t *testing.T) {

	type Foo struct {
		ID   int
		Name string
	}

	type Bar struct {
		ID   int
		Foos []Foo
	}

	var testCases = []struct {
		description string
		value       interface{}
		selectors   []string
		expect      int
	}{

		{
			description: "nexted selector selector",
			value: []*Bar{
				{
					ID: 1,
					Foos: []Foo{
						{ID: 1, Name: "abc1"},
						{ID: 0, Name: "xyz1"},
					},
				},
				{
					ID: 2,
					Foos: []Foo{
						{ID: 2, Name: "abc2"},
						{ID: 0, Name: "xyz2"},
						{ID: 0, Name: "xxx"},
					},
				},
			},
			selectors: []string{"Foos", "ID"},
			expect:    3,
		},
		{
			description: "slice selector",
			value: []*Foo{
				{ID: 1, Name: "abc"},
				{ID: 2, Name: "xyz"},
				{ID: 0, Name: "xyz"},
				{ID: 0, Name: "xyz"},
			},
			selectors: []string{"ID"},
			expect:    2,
		},
		{
			description: "object selector",
			value:       &Foo{ID: 0, Name: "abc"},
			selectors:   []string{"ID"},
			expect:      1,
		},
	}

	for _, testCase := range testCases[:1] {
		aWalker, err := NewWalker(testCase.value, testCase.selectors)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		actual, err := aWalker.CountEmpty(testCase.value)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}

func TestWalker_Leaf(t *testing.T) {

	type Foo struct {
		ID   int
		Name string
	}

	type Bar struct {
		ID   int
		Foos []Foo
	}

	var testCases = []struct {
		description string
		value       interface{}
		selectors   []string
		expect      interface{}
	}{

		{
			description: "nexted selector selector",
			value: []*Bar{
				{
					ID: 1,
					Foos: []Foo{
						{ID: 1, Name: "abc1"},
						{ID: 0, Name: "xyz1"},
					},
				},
				{
					ID: 2,
					Foos: []Foo{
						{ID: 2, Name: "abc2"},
						{ID: 0, Name: "xyz2"},
						{ID: 0, Name: "xxx"},
					},
				},
			},
			selectors: []string{"Foos", "ID"},
			expect:    &Foo{ID: 1, Name: "abc1"},
		},
		{
			description: "slice selector",
			value: []*Foo{
				{ID: 1, Name: "abc"},
				{ID: 2, Name: "xyz"},
				{ID: 0, Name: "xyz"},
				{ID: 0, Name: "xyz"},
			},
			selectors: []string{"ID"},
			expect:    Foo{ID: 1, Name: "abc"},
		},
		{
			description: "object selector",
			value:       &Foo{ID: 0, Name: "abc"},
			selectors:   []string{"ID"},
			expect:      &Foo{ID: 0, Name: "abc"},
		},
	}

	for _, testCase := range testCases[:1] {
		aWalker, err := NewWalker(testCase.value, testCase.selectors)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		actual, err := aWalker.Leaf(testCase.value)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}
