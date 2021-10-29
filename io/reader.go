package io

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	iocontext "github.com/viant/datly/io/context"
	"github.com/viant/datly/io/db"
	"github.com/viant/datly/metadata"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io"
	eio "io"
	"reflect"
	"sync"
)

//ReadInto reads data into target pointer
func ReadInto(ctx context.Context, target interface{}, aView *metadata.View, options ...config.Option) error {
	var conn, err = iocontext.Connector(ctx, aView.Connector, options...)
	if err != nil {
		return err
	}
	if aView.Connector == "" {
		aView.SetConnector(conn.Name)
	}
	var container Container
	container, err = NewContainer(target, aView)
	if err != nil {
		return err
	}
	session := iocontext.LookupSession(ctx)
	if session == nil {
		return fmt.Errorf("context.session was nil")
	}
	return readViewData(ctx, conn, aView, container)
}

func readViewData(ctx context.Context, conn *db.Connector, view *metadata.View, container Container) error {
	session := iocontext.LookupSession(ctx)
	session.RLock()
	group := sync.WaitGroup{}
	group.Add(1 + len(view.Refs))
	selector := view.Selector.Clone()
	session.RLock()
	err := selector.Apply(ctx, session.Input)
	session.RUnlock()
	SQL, parameters, err := view.BuildSQL(selector, session.Input)
	session.RUnlock()
	if err != nil {
		return err
	}

	reader, err := io.NewReader(ctx, conn.DB, SQL, func() interface{} {
		return container.New()
	}, conn.DB)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w, SQL: %s", err, SQL)
	}

	errors := &Errors{}
	go readRefViewsData(ctx, conn, view, &group, errors)
	err = reader.QueryAll(ctx, func(row interface{}) error {
		container.Add(row)
		return nil
	}, parameters...)
	if err != nil {
		return fmt.Errorf("failed to read %w, sql: %s", err, SQL)
	}
	group.Wait()



	if err = assignRefs(ctx, container, view); err != nil {
		errors.Add(err)
	}


	if view.OnRead != nil && errors.IsEmpty() {
		if err = container.Range(func(item interface{}) error {
			return view.OnRead.Visit(ctx, item)
		}); err == nil || err == eio.EOF {
			return nil
		}
		errors.Add(err)
	}

	if errors.IsEmpty() {
		return nil
	}
	return errors
}


func assignRefs(ctx context.Context, parentData Container, parentView *metadata.View) error {
	if len(parentView.Refs) == 0 {
		return nil
	}
	session := iocontext.LookupSession(ctx)
	for i := range parentView.Refs {
		ref := parentView.Refs[i]
		switch ref.Cardinality {
		case shared.CardinalityOne:
			aMap := session.Maps[ref.Name]
			keyFn := ref.KeyFn()
			getterFn := ref.Getter()
			return parentData.Range(func(item interface{}) error {
				key := keyFn(item)
				refItem, ok := aMap.Data[key]
				if !ok {
					return nil
				}
				ptr := getterFn(item)
				reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(refItem))
				return nil
			})

		default:
			multiMap := session.MultiMaps[ref.Name]
			keyFn := ref.KeyFn()
			getterFn := ref.Getter()
			return parentData.Range(func(item interface{}) error {
				key := keyFn(item)
				items, ok := multiMap.Data[key]
				if !ok {
					return nil
				}
				ptr := getterFn(item)
				container, err := NewStructContainer(ptr)
				if err != nil {
					return fmt.Errorf("failed to updated ref: %v, due to %w", ref.Name, err)
				}
				for i := range items {
					container.Add(items[i])
				}
				return nil
			})
		}
	}
	return nil
}

func readRefViewsData(ctx context.Context, conn *db.Connector, parent *metadata.View, group *sync.WaitGroup, errors *Errors) {
	defer group.Done()
	if len(parent.Refs) == 0 {
		return
	}
	//var err error
	for i := range parent.Refs {
		ref := parent.Refs[i]
		if ref.View() == nil {
			errors.Add(fmt.Errorf("ref view: %v was nil", ref.DataView))
			return
		}
		refConn := conn
		if ref.View().Connector != conn.Name {
			var err error
			refConn, err = iocontext.Connector(ctx, ref.View().Connector)
			if err != nil {
				errors.Add(err)
				return
			}
		}
		go readRefViewData(ctx, refConn, parent, ref, group, errors)
	}
}

func readRefViewData(ctx context.Context, conn *db.Connector, parent *metadata.View, ref *metadata.Reference, group *sync.WaitGroup, errors *Errors) {
	defer group.Done()
	session := iocontext.LookupSession(ctx)
	var collection Container
	if ref.Cardinality == shared.CardinalityOne {
		collection = session.NewMap(ref.View().ReflectType(), ref.Name, ref.RefKeyFn())
	} else {
		collection = session.NewMultiMap(ref.View().ReflectType(), ref.Name, ref.RefKeyFn())
	}
	refView, err := buildRefView(ctx, ref, parent)
	if err == nil {
		err = readViewData(ctx, conn, refView, collection)
	}
	if err != nil {
		errors.Add(err)
		return
	}
}

func buildRefView(ctx context.Context, ref *metadata.Reference, parent *metadata.View) (*metadata.View, error) {
	refView := ref.View().Clone()
	if ref.Strategy(parent.Connector) == metadata.RefStrategySQL {
		session := iocontext.LookupSession(ctx)
		selector := refView.Selector.Clone()
		selector.Columns = ref.Columns()
		SQL, parameters, err := parent.BuildSQL(selector, session.Input)
		if err != nil {
			return nil, err
		}
		refView.Params = parameters
		join := &metadata.Join{
			Type:  shared.JoinTypeInner,
			Alias: ref.Alias(),
			Table: "(" + SQL + ")",
			On:    ref.Criteria(refView.Alias),
		}
		refView.AddJoin(join)
	} else {
		//TODO optimize selection
		// Currently if different connector is used
		//-> wait for parent view data to apply ref view criteria
	}
	return refView, nil
}
