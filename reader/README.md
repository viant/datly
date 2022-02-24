###Reader usage

You can create Reader instance using constructor method. If all of your Views that uses reader are initialized by calling 
`View#Init` method, or by using `View` loaded by `NewResourceFromUrl` method, you can pass emptry Resource:

```go
emptyResource := data.EmptyResource()
service := reader.New(emptyResource)
```

Resource is needed only to try to initialize `View` unless `View` was initialized earlier.

In order to read data you need `data.Session`:

```go
var fooView *data.View 

session := &data.Session{
	Dest: new(interface{}),
	View: fooView, 
	AllowUnmapped: true,
	
	Selectors:     Selectors{},
	Subject:     "",
	HttpRequest: *http.Request{},
	MatchedPath: "",
}
```
* `Dest` is required. It has to be a pointer to `interface{}` or pointer to slice of `T` or `*T`
* `View` is required. Due to optimization reasons it is important to create one instance, share it across the system and provide fully initialized `View`.
For `View` creation see: [create programmatically](../go_usage.md) or [load from file](../yaml_usage.md)
* `Selectors` are not used when datly is used only to communicate with database, but may be used when datly is configured also as request handler (see [TODO](../TODO.md))
* `AllowUnmapped` is optional, by default is set to false. It is being used when some database table columns doesn't match `View.Schema` type.
* `Subject` is optional. It is being used to build criteria and represents logged user.
* `HttpRequest` is optional, needed only if any of `Parameter` location is `path`, `query`, `cookie`, `header` or if datly is used as request handler. 
* `MatchedPath` is optional, needed only if any of `Parameter` location is `path` or if datly is used as request handler. 
You can now read data from database using the reader instance.

You should let reader initialize the session. New session has to be created for each time you call `reader.Read`. Having reader and session you can fetch data from database:
```go
err := service.Read(context.TODO(), session)
if err != nil {
	// ... handle error
}

toolbox.DumpIndent(session.Dest, false) //prints data fetched from database
```