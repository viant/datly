## Cloud function runtime


#### Deployment


Before deployment modify [deploy.yaml](deploy.yaml) member section/or comment it and uncomment public  i.e

```yaml
members:
- user:${env.USER}@vindicotech.com
- ${gcp.serviceAccount}
```
On Terminal:

```bash
endly deploy authWith=$GCP_PROJECT (i.e viant-e2e)
```

####  Invoking cloud function



###### With gcloud

Generate identity token
```bash
 gcloud tmpl print-identity-token
 ```



###### With API 