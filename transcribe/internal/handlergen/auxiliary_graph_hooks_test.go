package handlergen

import (
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestAuxiliaryComponentHookGraphAdmission(t *testing.T) {
	for _, mode := range []string{"auxiliary", "writable", "cycle", "self"} {
		t.Run(mode, func(t *testing.T) {
			child := &plan.RecordPlan{Auxiliary: true}
			root := &plan.RecordPlan{Auxiliary: true, Relations: []*plan.RelationPlan{{Child: child}}}
			switch mode {
			case "writable":
				child.Auxiliary = false
			case "cycle":
				child.Relations = []*plan.RelationPlan{{Child: root}}
			case "self":
				child.SelfRelations = []plan.SelfRelationPlan{{}}
			}
			if allAuxiliaryHookGraph(root) != (mode == "auxiliary") {
				t.Fatal("incorrect canonical component hook graph admission")
			}
		})
	}
}
