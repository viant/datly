# Security 

## SQL Injection

To protect against SQL Injection datly uses prepared statements, allowing the database engine to handle the input parameters separately,
preventing malicious data from being interpreted as SQL commands.

### Dynamic criteria 

Datly allows client apply dynamic WHERE  criteria for specific queryable columns. In that case
WHERE clause is sanitized and all criteria values are converted into binding parameters.  

### Template Language

Datyl uses [velty](https://github.com/viant/velty) Java Velocity inspired template language, supporting basic control flow to
generate dynamically SQL/DML.
In datly all $Variable expressions are converted to SQL parameter placeholder.

Take the following snippet example
```sql
  INSERT INTO MY_TABLE(ID, NAME) ($Entity.ID, $Entity.Name)
```
will be replaced before calling database driver with
```sql
  INSERT INTO MY_TABLE(ID, NAME) (?, ?)
```

Input variable(s) can be also be accessed with $Unsafe namespace ($Unsafe.MyVariable), in that case variable is inlined.

### Authentication

Datly authenticate incoming request by verifying OAuth token. 
Data can be integrated with any OAuth provider like Google OAuth, Amazon Cognito or custom based on RSA public key or HMAC digest.   

Datly uses Oauth Identity token with JWT Claims verification with one of the following: 
- **config.JWTValidator** allows you to specify RSA, HMAC or Public OAth Certificate base authentication. 
- **config.Cognito** allows you to specify Cognito integration settings.

The following dsql examples, defines $Jwt header based parameter with JWTClaim codec 
and Authentication data view parameters to check if UserID from JWT Claims exists in USERS table.

```sql
#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Authentication<?>(data_view/Authentication)  /* !!401
   SELECT (TRUE) AS Authenticated  FROM USER WHERE ID = $Jwt.UserID
*/)
```


### Authorization



### API Keys

Datly support API keys as a means of authentication and access control for APIs.


### Handling secret



#### Securing secret

Datly integrates with [Scy - secure store api](https://github.com/viant/scy) when operating on credentials.


#### Securing database/sql DSN



In **dependencies** folder datly stores connection details make sure that before deploying to stage/prod all
credentials details are replaced with the following macros

```connections.yaml
Connectors:
    - DSN: ${Username}:${Password}@tcp(${Endpoint}:3306)/ci_ads?parseTime=true
      Driver: mysql
      Name: mydb
      Secret:
        URL: secure_storage_url
        Key:  blowfish://default
  - DSN: bigquery://my_org_project/myDataset?credURL=url_encoded_secure_storage_N_url
    Driver: bigquery
    Name: mybqdb
```

Where secure_storage_url could be any file system, including secret storage manager
- AWS SecretManager i.e. aws://secretmanager/us-west-2/secret/myorg/datly/e2e/mysql/mydb
- AWS SystemManager i.e. aws://ssm/us-west-1/parameter/MyOrgDatlyE2eMySQLMyDb
- GCP SecretManager i.e. gcp://secretmanager/projects/myorf-e2e/secrets/mysqlMyDB


To secure database credentials create [raw_credentials.json](asset/raw_credentials.json) file
and the use the following [scy](https://github.com/viant/scy) command

```bash
scy -s=raw_credentials.json -d=secure_storage_url -t=basic -k=blowfish://default
```

To secure Google Service Account Secret use the following [scy](https://github.com/viant/scy) command

```bash
scy -s=myServiceAccountSecret.json -d=secure_storage_url -t=raw -k=blowfish://default
```




