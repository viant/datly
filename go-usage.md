###Create Views programmatically

One of the most important concept used across the datly, are `Views`. `View `represents database table, and also allows
filtering rows and columns. You can load them from `.yaml` files, as one of the Resources:

```go
view := &View{
    Name: "foos",
    Connector: &config.Connector{
        Name: "mydb",
        Driver: "sqlite",
        DSN: "./mydb.db",
    }
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

You can assign `View` type by adding `Schema`:
```go
view := &View{
    Name: "foos",
    Connector: &config.Connector{
        Name: "mydb",
        Driver: "sqlite",
        DSN: "./mydb.db",
    },
    Schema: NewSchema(reflect.TypeOf(Foo{})),	
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

If no `Schema` is provided, `View` type will be created using database table column types. Iti s handy, but it allows you only to serialize objects. Type assertion will not be able to use.

You can explicitly say which columns do you want to fetch by specifying `Columns`, you need to also specify each `Column#DataType` 
```go
view := &View{
	Name: "foos",
	Connector: &config.Connector{
		Name: "mydb",
		Driver: "sqlite",
		DSN: "./mydb.db",
	},
	Columns: []*Column{
		{
			Name:          "id",
			DataType:      "Int",
		},
		{
			Name:          "quantity", 
			DataType:      "float", 
			Expression:    "round(quantity, 0)",
		},
	},
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

You can also explicitly tell which columns you don't want to expose:
```go
view := &View{
	Name: "foos",
	Connector: &config.Connector{
		Name: "mydb",
		Driver: "sqlite",
		DSN: "./mydb.db",
	},
    Exclude: []string{"id"},
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```
Examples above also shows one thing. In the case if neither `Table` nor `From` was specified, the `View name` will be used as database source. It can either be a table name like `foos` or it can be `select statement`.

You can also modify sql select statement by providing default values with `Selector` and `Criteria#Expression`:
```go
view := &View{
	Name: "foos",
	Connector: &config.Connector{
		Name: "mydb", 
		Driver: "sqlite",
		DSN: "./mydb.db",
	}, 
	Columns: []*Column{
		{
			Name:          "id", 
			DataType:      "Int",
		},
		{
			Name:          "quantity", 
			DataType:      "float", 
			Expression:    "round(quantity, 0)",
		},
	},
	Selector: &Config{
		OrderBy: "id", 
		Limit:   0,
	}, 
	Criteria: &Criteria{
		Expression: "quantity > 30",
	},
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

You can use `Client Selector` and specify what can be used using `Selector.Constraints`:

```go
view := &View{
	Name: "foos", 
	Connector: &config.Connector{
		Name:   "mydb", 
		Driver: "sqlite", 
		DSN:    "./mydb.db",
	}, 
	Selector: &Config{
        Constraints: &Constraints{
            Criteria:  ptrToBool(true),
            OrderBy:   ptrToBool(true),
            Limit:     ptrToBool(true),
            Columns:   ptrToBool(true),
            Offset:    ptrToBool(true),
        },
	},
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

You can use relations to assemble more complex structs - like `Event` and `EventType`:
```go
view := &View{
	Name: "events", 
	Connector: &config.Connector{
		Name:   "mydb", 
		Driver: "sqlite", 
		DSN:    "./mydb.db",
	}, 
	With: []*Relation{
		{
			Name: "event_event-type", 
			Of: &ReferenceView{
				View: View{
					Name: "event_types", 
					Connector: &config.Connector{
						Name:   "mydb", 
						Driver: "sqlite", 
						DSN:    "./mydb.db",
					},
				}, 
				Column: "id",
			}, 
			Cardinality:    "One", 
			Column:         "event_type_id", 
			Holder:         "EventType",
		},
	},
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

`With#Column` points to the table column `event_type_id` of the `event` table.
`Of#Column` points to the column `id` of the `event_type` table. Values of each of the columns must match each other in order to assemble `EventType` to the `Event` object.
The field that will hold the `EventType` will be created where name will be the same as `With#Holder`.
`With#Cardinality` indicates whether the `Holder` should be a single object (for cardinality `One`)
or slice of objects (for cardinality `Many`) . Both of the tables can be located in different databases.

Parameters allow changing sql statement depending on `*http.Request` or other `View`:
```go
view := &View{
	Name: "events", 
	Connector: &config.Connector{
		Name:   "mydb", 
		Driver: "sqlite", 
		DSN:    "./mydb.db",
	}, 
	Criteria: &Criteria{
		Expression: "id = ${user_id}",
	}, 
	Parameters: []*Parameter{
		{
			Name: "user_id", 
			In:   &Location{
				Kind: PathKind, 
				Name: "user-id",
			},
		},
	},
}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```
Parameters can be extracted from:
* cookie - `Kind: cookie`
* header - `Kind: header`
* query params - `Kind: query`
* path variable - `Kind: path`
* from database using another View - `Kind: data_view`. In this case - only one column has to be returned from database.

```go
view := &View{
		Name: "articles",
		Connector: &config.Connector{
			Name:   "mydb",
			Driver: "sqlite",
			DSN:    "./mydb.db",
		},
		MatchStrategy: ReadAll,
		With: []*Relation{
			{
				Name:        "articles_languages",
				Column:      "lang_id",
				Holder:      "Language",
				Cardinality: "One",
				Of: &ReferenceView{
					View: View{
						Name: "languages",
						Connector: &config.Connector{
							Name:   "otherdb",
							Driver: "sqlite",
							DSN:    "./mydb.db",
						}, 
						BatchReadSize: ptrToInt(4), 
						Selector: &Config{
							Limit: 14,
						},
					},
					Column: "id",
				},
			},
		},
	}

err := view.Init(context.TODO(), EmptyResource())
if err != nil {
//...handle error
}
```

Default `MatchStrategy` is `read_matched` - referenced View will wait until Parent fetches result and will narrow fetched result
to only needed records (by `Column in (?,?,?)`). The other supported is `read_all` - the all specified parent values and all referenced view values will be fetched at the same time
then filtered and merged at the backend. You can also specify `BatchReadSize` - in example above, datly will fetch no more than 14 languages
in chunks no bigger than 4.

###Create Resource programmatically
If you want to load `Views` from yaml file, you are doing it indirectly by loading `Resource` and then you can find `View` by its name.
You can also create `Resource` programmatically:

First, you need to prepare Types and create empty `Resource` with specified types:
```go
type Foo struct {
	Id    int
	Name  string
	Price float64
}

types := Types{}
types.Register("foo", reflect.TypeOf(Foo{}))
resource := NewResource(types)
```

Then you need to create and register `Views`:
```go
fooView := &View{
	Name: "foos",
	Connector: &config.Connector{
		Reference: shared.Reference{
			Ref: "mydb",
		},
	},
	Parameters: []*Parameter{
		{
			Reference: shared.Reference{
				Ref: "user_id",
			},
		},
	},
	Schema: &Schema{
		Name: "foo",
	},
}
	
fooViewInherited := &View{
	Reference: shared.Reference{
		Ref: "foos",
	},
	Name:  "foos_referenced", 
	Table: "foos",
}	
resource.AddViews(fooView, fooViewInherited)
```

Then you need to create and register `mydb` connector used by View `foos`.
```go
mydbConnector := &config.Connector{
	Name:   "mydb",
	DSN:    "./testdata/db/mydb.db",
	Driver: "sqlite3",
}
resource.AddConnectors(mydbConnector)
```

And also you need to create and register `user_id` parameter used by `foos`.
```go
userIdParameter := &Parameter{
	Name: "user_id",
	In: &Location{
		Kind: QueryKind,
		Name: "user-id",
	},
}
resource.AddParameters(userIdParameter)
```
At the end you need to call `Init` on `Resource`:
```go
err := resource.Init(context.TODO())
if err != nil {
	//handle error
}
```
After initialization `Resource`, all `Views`, `Connectors` and `Parameters` will be fully initialized and ready to use,
including `fooView`, `fooViewInherited`, `mydbConnector`, and `userIdParameter`.