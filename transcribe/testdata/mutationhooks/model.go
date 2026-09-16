package mutationhooks

import (
	"context"
	"fmt"

	h "github.com/viant/xdatly/handler"
)

type RowHas struct{ ID, Name, Children bool }
type ChildHas struct{ ID, ParentID, Name, Self bool }
type Row struct {
	ID       int      `sqlx:"id,primaryKey=true"`
	Name     string   `sqlx:"name"`
	Children []*Child `sqlx:"-"`
	Has      *RowHas  `setMarker:"true" sqlx:"-"`
}
type Child struct {
	ID       int       `sqlx:"id,primaryKey=true"`
	ParentID int       `sqlx:"parent_id"`
	Name     string    `sqlx:"name"`
	Self     []*Child  `sqlx:"-"`
	Has      *ChildHas `setMarker:"true" sqlx:"-"`
}
type Input struct {
	Rows             []*Row   `parameter:"Rows,kind=body,in=data,required"`
	Suffix           string   `parameter:"Suffix,kind=query,in=suffix"`
	ExpectName       bool     `parameter:"ExpectName,kind=query,in=expectName"`
	Previous         []*Row   `parameter:"Previous,kind=view,in=Previous" view:"Previous,table=records" sql:"SELECT id,name FROM records ORDER BY id"`
	PreviousChildren []*Child `parameter:"PreviousChildren,kind=view,in=PreviousChildren" view:"PreviousChildren,table=children" sql:"SELECT id,name FROM children ORDER BY id"`
}

func (row *Row) SetName(value string) {
	row.Name = value
	if row.Has == nil {
		row.Has = &RowHas{}
	}
	row.Has.Name = true
}
func (child *Child) SetName(value string) {
	child.Name = value
	if child.Has == nil {
		child.Has = &ChildHas{}
	}
	child.Has.Name = true
}

type Output struct{ Data []*Row }

type RootHooks struct {
	Input       *Input   `bind:"kind=input,required"`
	Logger      h.Logger `bind:"kind=logger,required"`
	initialized map[*Row]bool
}

func (hook *RootHooks) Init(_ context.Context, row *Row, state h.LifecycleContext[Row, h.NoParent, Output]) error {
	if hook.Input == nil || hook.Logger == nil {
		return fmt.Errorf("canonical DI missing")
	}
	if state.Parent != nil || state.SelfParent != nil {
		return fmt.Errorf("root parent state is wrong")
	}
	if state.Previous == nil || state.Previous.Name != "database" || state.PreviousFields == nil || !state.PreviousFields.Has("Name") || state.PreviousFields.Has("Unloaded") {
		return fmt.Errorf("database previous/projection is wrong")
	}
	if state.Original == nil || !state.Original.Available() || state.Original.Has("Name") != hook.Input.ExpectName {
		return fmt.Errorf("original presence missing")
	}
	if hook.initialized == nil {
		hook.initialized = map[*Row]bool{}
	}
	hook.initialized[row] = true
	row.SetName(state.Previous.Name + ":" + hook.Input.Suffix)
	hook.Logger.Debug("root init")
	return nil
}
func (hook *RootHooks) Validate(_ context.Context, row *Row, state h.LifecycleContext[Row, h.NoParent, Output]) error {
	if !hook.initialized[row] {
		return fmt.Errorf("root hook instance changed")
	}
	if row.Has == nil || !row.Has.Name || state.Original.Has("Name") != hook.Input.ExpectName {
		return fmt.Errorf("setter presence/original snapshot changed")
	}
	hook.Logger.Debug("root validate")
	if hook.Input.Suffix == "reject" {
		return fmt.Errorf("entity validation rejected")
	}
	return nil
}

type ChildHooks struct {
	Input       *Input   `bind:"kind=input,required"`
	Logger      h.Logger `bind:"kind=logger,required"`
	initialized map[*Child]bool
}

func (hook *ChildHooks) Init(_ context.Context, child *Child, state h.LifecycleContext[Child, Row, Output]) error {
	if hook.Input == nil || hook.Logger == nil || state.Parent != hook.Input.Rows[0] {
		return fmt.Errorf("child parent/DI is wrong")
	}
	if state.Previous == nil || state.Previous.ID != child.ID || state.PreviousFields == nil || !state.PreviousFields.Has("ID") {
		return fmt.Errorf("child previous/projection is wrong")
	}
	if state.Original == nil || !state.Original.Has("Name") {
		return fmt.Errorf("child original missing: id=%d working marker=%+v original=%+v", child.ID, child.Has, state.Original)
	}
	if child.ID == 11 && state.SelfParent != state.Parent.Children[0] {
		return fmt.Errorf("recursive child self parent missing")
	}
	if child.ID == 10 && state.SelfParent != nil {
		return fmt.Errorf("self tree root parent must be nil")
	}
	if hook.initialized == nil {
		hook.initialized = map[*Child]bool{}
	}
	hook.initialized[child] = true
	child.SetName(state.Parent.Name + ":child")
	hook.Logger.Debug("child init")
	return nil
}
func (hook *ChildHooks) Validate(_ context.Context, child *Child, _ h.LifecycleContext[Child, Row, Output]) error {
	if !hook.initialized[child] {
		return fmt.Errorf("child hook instance changed")
	}
	hook.Logger.Debug("child validate")
	return nil
}

type ScalarHooks int

func (hook *ScalarHooks) Init(context.Context, *Row, h.LifecycleContext[Row, h.NoParent, Output]) error {
	*hook++
	return nil
}
func (hook *ScalarHooks) Validate(context.Context, *Row, h.LifecycleContext[Row, h.NoParent, Output]) error {
	if *hook != 2 {
		return fmt.Errorf("scalar hook instance changed: %d", *hook)
	}
	return nil
}
