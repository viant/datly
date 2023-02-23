
- updated doc/ with Read DSQL


- remove usage of {"Auth":"Jwt"} with explicit declaration  and codec ?
  #set($_ = $Jwt<JwtClaim>)(Header/Authorization).WithCodec('ABC') -- <  {"Auth":"Jwt"}
  also introduce to reader  with optional custom import (if dedicated datly switch used) ?
  Customze reader data type (OnFetch)
- Extend route option with service kind


- Async mode 
  - post job, SQL-> (job id , dispotistion -> dest_teable,  temp_table)
  - job status <- job-id (RUNNING/ERROR/DONE)
  - post http, message, storage event




- Pre dsql enhancement

```sql
         SELECT  '' AS transient_pseudo_column, 
          f.ID  AS ID /* go struct tag support here */ f.*  FROM foo f 
  ```
- batch spreedsheet ingestion/response
- XML input / output
- Parameter Criteria IN, EXISTS, etc

- IntelJ plugin enhancement

- multi transaction support on velthy ?
- Add possibility for sqlx ns in order to avoid unneccessary one-one relations.


- performance profiling/tuning
- cache metric add detail response about each view case time,records, etc...
- more than one view in out output nice to have for now
- improve documentation/examples
- add support for composite keys

- ChatGPT integration

- Management API
- add GUI

