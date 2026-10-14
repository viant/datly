# Shared handler routes

A DQL component can declare multiple paths with repeated `$route` settings. The
paths share one component owner, handler factory, generated Input and Output,
and SQL-derived body graph. Generate the component once with its usual operation.

This directive fragment belongs in the same DQL as the factory and contract:

```sql
#setting($_ = $route('/records/archive','PATCH'))
#setting($_ = $route('/records/restore','PATCH'))

#setting($_ = $handler_factory('records.NewHandler','Records'))
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
```

When behavior depends on the path, declare the existing HTTP request URI input
and inspect it in the handler:

```sql
#define($_ = $Uri<string>(http_request/uri))
```

Request-body mode, API key and visibility settings apply to every declared path,
regardless of where those settings appear among the route declarations. Path
parameters are inferred and validated for every path. Duplicate method/path pairs
are rejected. The generated holder contains a typed contract field for each route;
package discovery and reload retain all routes and the same Go contract types.

The routes share a component-derived observability name. Preserve a required
distinct route name separately when migrating an existing contract; shared paths
alone do not establish logging or MCP name parity. A single MCP declaration is
ambiguous across multiple routes and is rejected. Static content does not support
multiple path declarations. `WithURI` remains input activation metadata and is not
needed merely to give one handler multiple paths.

Keep one DQL owner for shared generated contracts. Do not generate independent
components into the same Input/Output files, relax ownership checks, add extra
holders by hand, or edit generated routes after transcription. Separate components
can still reuse genuine shared Go types through their normal supported bindings.
