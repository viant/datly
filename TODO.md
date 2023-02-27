- Add possibility for sqlx ns in order to avoid unneccessary one-one relations.
- SQL with StructQL simplificaiton ?
- Executor triggers/notification
- 
- Async mode 
  - post job, SQL-> (job id , dispotistion -> dest_teable,  temp_table)
  - job status <- job-id (RUNNING/ERROR/DONE)
  - post http, message, storage event

- setting customization for (async batch mode - disable by default)

- remove usage of {"Auth":"Jwt"} with explicit declaration  and codec ?
  #set($_ = $Jwt<JwtClaim/)(Header/Auth) -- <  {"Auth":"Jwt"} 

  
- batch spreedsheet ingestion/response

- multi transaction support on velthy ?
- XML input / output

- Parameter Criteria IN, EXISTS, etc


- performance profiling/tuning
- cache metric add detail response about each view case time,records, etc...
- more than one view in out output nice to have for now
- improve documentation/examples
- add support for composite keys

- Management API
- add GUI

