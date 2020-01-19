## End to end test


## Prerequisites

##### Installation

1. Install endly e2e runner as [binary](https://github.com/viant/endly/releases) or endly docker image:

This instruction was prepared with endly version 0.46.2

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

1. Create mysql credentials:

```bash
# use user root with password dev
endly -c=mysql-e2e  
```


