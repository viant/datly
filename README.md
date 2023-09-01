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

Datly is a modern and flexible ORM and data management platform designed with three principles in mind: **performance**, **productivity**, and **security**.
Datly is SQL-centric, where data comes first.

**Productivity** is achieved by using a higher 4th generation language called DSQL (Datly SQL dialect) to address common problems of manipulating data, 
allowing developers to focus on addressing business requirements. 
In addition, more complex cases can be easily delegated to pure Golang, where Datly intermediates with data access and modification. 
Higher abstraction languages promotes development consistency, offload developers from writing the same code over and over again, which includes routing, struct mapping, batching, 
security handling, common validation, pagination, dynamic field selection, dynamic criteria, data encoding: json,json-tabular, csv, caching, scaling, runtime/platform independence, sending 
notification vi universal message bus (sqs/sns/kafka/pubsub) and more.

Datly promotes data cohesion with grouping/batching operation. 
For example  to boostrap your patch operation you would first  analyze all inputs driving business logic, 
then define patch source generation SQL  with only needed data points to generate initial patch rule, for example


```sql
SELECT  Products.* /* { "Cardinality": "One", "Field":"Entity" } */,
        ProductFlights.*,
        Vendor.*,
        Acl.*,
        Features.*
FROM (SELECT * FROM PRODUCTS) Products,
LEFT JOIN (SELECT * FROM PRODUCT_FLIGHTS) ProductFlights WHERE ProductFlights.PRODUCT_ID = Products.ID
LEFT JOIN (SELECT ID, 
                CURRENCY_ID,
                (SELECT ctz.IANA_TIMEZONE FROM TIME_ZONE ctz WHERE v.TIME_ZONE_ID = ctz.ID) AS IANA_TIMEZONE
            FROM Vendor v
) Vendor  ON Vendor.ID = Product.VENDOR_ID AND 1=1
LEFT JOIN (
    SELECT ID USER_ID,
           HasUserRole(ID, 'ROLE_READ_ONLY') AS IS_READ_ONLY,
           HasUserRole(ID, 'ADMIN') AS IS_ADMIN
    FROM (USERS)
) Acl ON Acl.USER_ID = Products.USER_ID AND 1=1
LEFT JOIN (SELECT
         ID USER_ID,
         HasFeatureEnabled(ID, 'EXPOSE_FEATURE_1') AS FEATURE_1,
        HasFeatureEnabled(ID, 'EXPOSE_FEATURE_2') AS FEATURE_2
        FROM (USERS)
) Features ON Features.USER_ID = Products.USER_ID AND 1=1
```

In the example above Products, Flights and Vendor represents previous state, Acl defines access-control list, 
and Features represents feature activator in the UI application.


While Datly in autonomous mode purely uses a meta-driven approach, custom Datly allows blending Go-developed code into rules.
As opposed to the purely meta-driven approach, Datly allows both modes to be debugged and troubleshooted with traditional debuggers.
Datly automatically generates openAPI documentation allowing any programing languages integrated seamlessly with Datly based micro/rest services.
Datly is runtime agnostic, and it can be deployed as standalone, serverless (lambda, cloud function), or Dockerized.
Datly is deployment time optimized, allowing rule and logic deployment with powerful Go plugins under seconds on Lambda and other serverless cloud platform.


**Performance** is achieved by utilizing Go with GoLang structs (never maps), while other frameworks manipulating data use Go reflection, 
which is around 100x slower than natively typed code,  Datly uses [xunsafe](https://github.com/viant/xunsafe) custom Go reflection, which is only around 5x slower than natively typed code.
Datly has the ability to read and assemble data from various database vendors at once and provides powerful optimization techniques like seamless smart caching, 
driving both client performance and substantially reducing cost. 
Datly uses Velocity inspired [velty](https://github.com/viant/velty) templating language which is one of the fastest in the whole Go echo system.
On average velty is 20x faster than go Text/template and 8-15x faster than JDK Apache Velocity

Datly can operate on both SQL and NoSQL databases. Large datasets (e.g., BigQuery) can be cached pre-warmed up without engineers writing a single line of code. 
Datly comes with powerful metrics that provide execution time breakdowns for each data access operation.

When it comes to data modification, Datly can leverage seamless batch and load operations, speeding up data ingestion by 25-50x compared to traditional insert techniques. 
Datly provides an easy way to build POST/PUT/DELETE and truly performant PATCH operations.
Datly use modification marker to distinct input state, allowing handling user input effectively, ensuring data integrity, and improving the security of applications.


**Security**
Datly is secure. It's resilient against SQL injection attacks. 
On top of that, it promotes secure secrets storage natively with all database/sql drivers. 
Finally, it's integrated with OAuth, which provides a convenient way for both controlling authentication and row and column based authorization.

See more [Datly secutity](doc/security/README.md)


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

To test dsql vi reset endpoint run the following command
```bash
datly dsql -c='dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true' -s=dept.sql -P=8080
open http://127.0.0.1:8080/v1/api/dev/dept    
```

To persist rule and then run datly run the following
```bash
datly dsql -c='mydb|mysql|myusser:mypass@tcp(127.0.0.1:3306)/mydb?parseTime=true' -s=dept.sql -r=reop/dev
datly run -c=proj/Datly/config.json
```

To see go struct generated for the view run the following
```bash
open http://127.0.0.1:8080/v1/api/meta/struct/dev/dept
```

To see go openapi for the view run the following
```bash
open http://127.0.0.1:8080/v1/api/meta/openapi/dev/dept
```




## Usage

#### Managed mode

For reader usage, see: [how to use reader](service/reader/README.md) 
For executor usage, see: [how to use executor](service/executor/README.md)

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

