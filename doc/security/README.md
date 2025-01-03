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


The following dql examples, defines $Jwt header based parameter with JWTClaim codec 
and Authentication data view parameters to check if UserID from JWT Claims exists in USERS table.

```sql
#set($_ = $Jwt<string>(Header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Authentication<?>(data_view/Authentication)  /* !!401
   SELECT (TRUE) AS Authenticated  FROM USER WHERE ID = $Jwt.UserID
*/)
```

'!' - mean that query has to return at least one record otherwise datly return specified error code in that case 401


### Authorization


### API Keys

Datly support API keys as a means of authentication and access control for APIs.


### Handling secret


#### Securing secret

Datly integrates with [Scy - secure store api](https://github.com/viant/scy) when operating on credentials.


#### Securing database/sql DSN

When connector parameter is use instead of hardcoding credentials
```bash
-c='name|mysql|root:dev@tcp(127.0.0.1:3306)/demo?parseTime=true'
```

use the [scy](https://github.com/viant/scy) secure alternative

```bash
-c='name|mysql|${Username}:${Password}@tcp(127.0.0.1:3306)/demo?parseTime=true|secure_storage_url|blowfish://default'
```


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


### Oauth configuration


JWTValidator section in datly repository config define OAuth setting, 
or Cognito to get certificate generate for the AWS user pool.

```json
{
  "JWTValidator": {
    "RSA": {
      "URL": "public_key_url.enc",
      "Key": "blowfish://default"
    },
    "HMAC": {
      "URL": "hmac_url.enc",
      "Key": "blowfish://default"
    },
    "CertURL": "public_cert_url"
  },
  "Cognito": {
    
  }
}
```

#### Custom RSA private/public key

1. Generate RSA key
```bash
# Created Private key
openssl genpkey -out private.txt -outform PEM -algorithm RSA -pkeyopt rsa_keygen_bits:4096
#  Created public key with:
openssl pkey -inform PEM -outform PEM -in private.txt -pubout -out public.txt
```

Secure both key with scy:

```bash
    scy -m=secure -s=public.txt -d=public_key_url.enc -t=raw -k=blowfish://default ## on prod, use secure store instead of local fs

    ### in case you want to generate test token please also secure private key
    scy -m=secure -s=private.txt -d=private_key_url.enc -t=raw -k=blowfish://default ## on prod, use secure store instead of local fs
```

#### Custom HMAC key

1. Generate HMAC key

```bash
openssl rand -base64 256 -out hmac.txt
```
```bash
    scy -m=secure -s=hmac.txt -d=hmac.enc -t=raw -k=blowfish://default ## on prod, use secure store instead of local fs

```

### Generating JWT token 

Creates jwt [Claim](https://github.com/viant/scy/blob/main/auth/jwt/claims.go) in JSON format
```bash
   echo '{"user_id":123,"email":"dev@viantinc.com"}' > claims.json 
```

#### Custom RSA

Sign claim
```bash
   scy -m=signJwt -s=claims.json -e=600 -r=private.scy -k=blowfish://default
```

To verify JWT Claim
```bash
   scy -m=verifyJwt -s=token.json -r=public.enc -k=blowfish://default
``` 

#### Custom HMAC

Sign claim

```bash
   scy -m=signJwt -s=claims.json -e=600 -r=private.scy -k=blowfish://default
``` 

To verify JWT Claim
```bash
   scy -m=verifyJwt -s=token.json -a=hmac.enc -k=blowfish://default
``` 



