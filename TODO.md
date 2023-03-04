
- Executor triggers/notification (notification message ID in response)
-> events ->

-> logging -> { "Status": "ok", "EventSourceId":"", "EventSource": "Advertier", "EventType":"Insert" "Data":interface{}, "UserId":"", "TraceID":""    }

-> Logger miner ->
   log files, position
    -> mathch, action execution
    ->




- Async mode 
  - post job, SQL-> (job id , dispotistion -> dest_teable,  temp_table)
  - job status <- job-id (RUNNING/ERROR/DONE)
  - post http, message, storage event

- programatic router (template with ability to reroute)
- self documents

- batch spreedsheet ingestion/response


- multi transaction support on velthy ?
- XML input / output
- enhence sqlx ns in order to avoid unneccessary one-one relations.
- Parameter Criteria IN, EXISTS, etc
- SQL with StructQL simplification


- performance profiling/tuning
- cache metric add detail response about each view case time,records, etc...
- more than one view in out output nice to have for now
- improve documentation/examples
- add support for composite keys
- setting customization for (async batch mode - disable by default)

- Management API
- add GUI

- Validator
- Warning
