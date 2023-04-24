# Datly - Modern flexible ORM for rapid development

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/datly)](https://goreportcard.com/report/github.com/viant/datly)
[![GoDoc](https://godoc.org/github.com/viant/datly?status.svg)](https://godoc.org/github.com/viant/datly)


This library is compatible with Go 1.11+

Please refer to [`CHANGELOG.md`](../CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Usage](#usage)
- [License](#license)


## Motivation

The goal of this project is to simplify and speed up data layer prototyping and development.
It can be used as golang ORM or purely rule based.
This is achieved by utilising rules to govern data mapping and binding for all data interaction.

## Introduction

Datly has been design as modern flexible ORM for rapid development, 
allowing reading/transforming, and changing data with POST/PUT/PATCH/DELETE operation.

Datly can be used as regular ORM or in mode autonomous (rule based) mode.

Datly use [dsql](doc/README.md#datly-sql--dsql-) to auto generate struct or internal datly rule



**dept.sql**
```sql
SELECT 
    dept.* EXCEPT ORG_ID
    employee.* EXCEPT DEPT_ID, 
    organization.* 
FROM (SELECT * FROM DEPARMENT t) dept
JOIN (SELECT ID, NAME, DEPT_ID FROM EMP t) employee ON dept.ID = employee.DEPT_ID
JOIN ORG organization ON organization.ID = demp.ORG_ID AND 1=1
```

The following command bootstrap 

```bash
datly -C='mydb|mysql|myusser:mypass@tcp(127.0.0.1:3306)/demo?parseTime=true' -X dept.sql
open http://127.0.0.1:8080/v1/api/dev/dept    
```




## Usage

#### Managed mode

For reader usage, see: [how to use reader](./reader/README.md) 
For executor usage, see: [how to use executor](./executor/README.md)

#### Autonomus mode

## Contributing to datly

Datly is an open source project and contributors are welcome!

See [TODO](./TODO.md) list

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Authors:** 
- Kamil Larysz
- Adrian Witas

