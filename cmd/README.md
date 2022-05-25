## Command line datly executor


```go
./datly -h
```

#### Generate rule with endpoint for a table

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName
```

#### Generate rule with endpoint for a table and SQL

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName -S=view.sql
```

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName
```

#### Generate rule with endpoint for a table and relations

```bash
 ./datly  -C=dev  -N=MyViewName -T=MyTableName  -R=MyRelName:RelTable 
```
