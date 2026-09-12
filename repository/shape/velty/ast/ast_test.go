package ast

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestBlock_Stringify(t *testing.T) {

	var testCases = []struct {
		description string
		block       Block
		options     Options
		expect      string
	}{
		{
			description: "assign",
			options:     Options{Lang: LangVelty},
			block: Block{
				&Assign{Holder: &Ident{Name: "inited"}, Expression: &CallExpr{Receiver: Ident{Name: "Campaign"}, Name: "init", Args: []Expression{
					Ident{Name: "CurCampaign"},
				}}},
			},
			expect: `#set($inited = $Campaign.init($CurCampaign))`,
		},
		{
			description: "for each ",
			options:     Options{Lang: LangVelty},
			block: Block{
				&Foreach{Set: &Ident{Name: "Sets"},
					Value: &Ident{Name: "Item"},
					Body: Block{
						&Assign{Holder: &Ident{Name: "tested"}, Expression: &CallExpr{Receiver: Ident{Name: "Campaign"}, Name: "Test", Args: []Expression{
							Ident{Name: "Item"},
						}}}}}},
			expect: `#foreach($Item in $Sets)
  #set($tested = $Campaign.Test($Item))
#end`,
		},
		{
			description: "if condition",
			options:     Options{Lang: LangVelty},
			block: Block{
				&Condition{
					If: &BinaryExpr{X: &Ident{Name: "Campaign.Id"}, Op: ">", Y: &LiteralExpr{Literal: "1"}},
					IFBlock: Block{
						&Assign{Holder: &Ident{Name: "inited"}, Expression: &CallExpr{Receiver: Ident{Name: "Campaign"}, Name: "init", Args: []Expression{
							Ident{Name: "CurCampaign"},
						}}},
					},
					ElseIfBlocks: []*ConditionalBlock{{
						If: &BinaryExpr{X: &Ident{Name: "Campaign.Name"}, Op: "==", Y: &LiteralExpr{Literal: `"Foo"`}},
						Block: Block{
							&Assign{Holder: &Ident{Name: "fooed"}, Expression: &CallExpr{Receiver: Ident{Name: "Campaign"}, Name: "Foo", Args: []Expression{
								Ident{Name: "CurCampaign"},
							}}},
						},
					},
					},
				},
			},
			expect: `#if($Campaign.Id > 1)
  #set($inited = $Campaign.init($CurCampaign))
#elseif($Campaign.Name == "Foo")
  #set($fooed = $Campaign.Foo($CurCampaign))
#end`,
		},
		{
			description: "assign condition | go",
			options:     Options{Lang: LangGO},
			block: Block{
				&Assign{Holder: &Ident{Name: "foo"}, Expression: &LiteralExpr{Literal: "10"}},
			},
			expect: `foo := 10`,
		},
		{
			description: "if stmt | go",
			options:     Options{Lang: LangGO},
			block: Block{
				&Condition{
					If: &BinaryExpr{X: &LiteralExpr{"0"}, Y: &Ident{Name: "foo"}, Op: ">"},
					IFBlock: Block{
						&Assign{Holder: &Ident{Name: "foo"}, Expression: &BinaryExpr{X: &Ident{Name: "foo"}, Op: "*", Y: &LiteralExpr{Literal: "-1"}}},
					},
				},
			},
			expect: `if 0 > foo {
  foo = foo * -1
}`,
		},
		{
			description: "foreach",
			options:     Options{Lang: LangGO},
			block: Block{
				&Foreach{
					Value: &Ident{Name: "foo"},
					Set:   &Ident{Name: "foos"},
					Body: Block{
						&CallExpr{
							Receiver: &Ident{Name: "fmt"},
							Name:     "Printf",
							Args:     []Expression{&Ident{Name: "foo"}},
						},
					},
				},
			},
			expect: `for _, foo := range foos { 
  fmt.Printf(foo)
}`,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		builder := NewBuilder(testCase.options)
		err := testCase.block.Generate(builder)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		actual := builder.String()
		assert.EqualValues(t, testCase.expect, strings.TrimSpace(actual))
	}
}
