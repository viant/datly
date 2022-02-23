#Create Views programmatically

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

You can use `Client Selector` and specify what can be used using `SelectorConstraints`:

```go
view := &View{
	Name: "foos", 
	Connector: &config.Connector{
		Name:   "mydb", 
		Driver: "sqlite", 
		DSN:    "./mydb.db",
	}, 
	SelectorConstraints: &Constraints{
		Criteria:  ptrToBool(true), 
		OrderBy:   ptrToBool(true), 
		Limit:     ptrToBool(true), 
		Columns:   ptrToBool(true), 
		Offset:    ptrToBool(true),
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