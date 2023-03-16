- code parameter assets (i.e xml validator)

- pre proxy processor/programatic router  
 #if($reportName='abc')
  $proxy.To('/v1/api/ws/view1')
 #else if($reportName='xyz')
  $proxy.To('/v1/api/ws/view2')
 #end

- Executor triggers/notification (notification message ID in response)
-> logging -> { "Status": "ok", "EventSourceId":"", "EventSource": "Advertier", "EventType":"Insert" "Data":interface{}, "UserId":"", "TraceID":""    }
-> Logger miner ->
   log files, position
    -> match, action execution

- Async mode 
  - post job, SQL-> (job id , dispotistion -> dest_teable,  temp_table)
  - job status <- job-id (RUNNING/ERROR/DONE)
  - post http, message, storage event
 
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
- ChatGPT integration
