## End to end test


## Prerequisites

This project uses [endly](https://github.com/viant/endly/) end to end test runner.

##### Installation

1. Install endly e2e runner as [binary](https://github.com/viant/endly/releases) or endly docker image:

This instruction was prepared with endly version 0.47.0

**Docker**

To use endly docker images run the following:

```bash 
docker run --name endly -v /var/run/docker.sock:/var/run/docker.sock -v ~/e2e:/e2e -v ~/e2e/.secret/:/root/.secret/ -p 7722:22  -d endly/endly:latest-ubuntu16.04  
ssh root@127.0.0.1 -p 7722 ## password is dev
## to check endly version
endly -v
endly -c=localhost #provide user root and password dev
```


**Local machine**

To download endly on your machine use the following [link](https://github.com/viant/endly/releases)
Once endly is installed on your machine enable and add [SSH Credetials](https://github.com/viant/endly/tree/master/doc/secrets#ssh)

##### Setting up test credentials

**Create local database credentials:**
```bash
# use user root with password dev
endly -c=mysql-e2e  
# use user root with password dev
endly -c=pq-e2e

```
**Create AWS databases/credentials (optional)**
- Create RDS MySQL with password based auth and public endpoint (testing only)
- Create credentials file
```bash
endly -c=aws-mysql-e2e
````
- Modify credential ~/.secret/aws-mysql-e2e.json file with database Endpoint
@~/.secret/aws-mysql-e2e.json
```json
{ "Endpoint": "db.xxxxxxx.us-west-1.rds.amazonaws.com", "Username":"root","EncryptedPassword":"*****"}
```


##### Clone the this project:
```bash
git clone https://github.com/viant/datly.git
cd datly/e2e
```

Update datly/e2e/run.yaml:

To enable AWS test cases update the following:
- awsConfigBucket 
- runOnAws set to true (false by default)

When using endly docker image set
- useDockerDBIP



## Use cases

To run all test use the following command:

```bash
cd datly/e2e
endly run.yaml
```

To run individual use cases run first init task,  followed by individual case run.

```json
endly -t=init
```

- [Basic Data View](regression/cases/001_basic)

```bash
    endly -t=test -i=basic
```

- [Data View References](regression/cases/002_refs)

```bash
    endly -t=test -i=refs
```


- [Data View Templates](regression/cases/003_templates)

```bash
    endly -t=test -i=templates
```


- [Data View Case Format Control](regression/cases/004_case_format)

```bash
    endly -t=test -i=case_format
```

- [SQL based Data View](regression/cases/005_sql)

```bash
    endly -t=test -i=sql
```

- [AWS API Gateway Case](regression/cases/006_apigw_reader)

```bash
    endly -t=test -i=apigw_reader
```


