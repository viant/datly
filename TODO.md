- ${[view|parentView].[Name|Limit|Offset|SQL|NonWindowSQL]}

- 
  - type ViewInfo  stuct {
      Name string
      Limit int
      Offset int
      SQL string
      NonWindowSQL string
  }




- context based generator enhancement
- reduce complexity/execution branches/duplication on selector parameters
- pagination - It1: only when data is read from the cache, flag enable by default
- Simplify/remove lifecycle,  simplify codec

- ($parentView.Limit / $parentView.Offset)
- e2e / generator refactoring, remove unused/duplicated options
- AuditLog -> logger with velty template
- Parameter Criteria IN, EXISTS, etc 
- XML/CSV output
- performance profiling/tuning
- strinigfy golang type in meta/view endpoint
- cache metric add detail response about each view case time,records, etc...
- insert service(with batch mode)/support for semi-autonomous mode (registry hooks)
- patch/create/update
- parameters with golang/js codec
- more than one view in out output nice to have for now
- improve documentation/examples

- velty #query
- velty macros
- Management API 
- add GUI

- add template meta to caching
- limit / offset window option with SQLX
- Markers for pagination data, 100, 1000, 10000 etc.