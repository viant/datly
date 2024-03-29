### Reader

Reader service provide ability to read view data.

- [Introduction](#introduction)
- [Usage](#usage)
  - [Reading view data](#reading-view-data)
  - [Reading view data from various databases](#reading-view-data-from-various-databases)
  - [Reading data with SQL template](#reading-data-with-sql-template)
  - [Reading data with lifecycle hooks](#reading-data-with-lifecycle-hooks)

#### Introduction

A reader can read a single view or view that has complex structure with to many or to one relations.
A [view](../../view) represents a database table, SQL or SQL Template. For multi relation view each view data can be store
in different database vendor. 
Underlying view can provided or autogenerated, datly will map all struct Field using sqlx tag.

You can autogenerated view struct using [dql](../../gateway/doc.go#datly-sql-dql)

For example to  generate invoice go struct, run the following command:

**invoice.sql**
```sql
SELECT
invoice.*,
item.*
FROM (SELECT * FROM invoice) invoice
JOIN (SELECT * FROM invoice_list_item) item ON item.invoice_id = invoice.id
```

```bash
    datly -C='demo|mysql|demo:demo@tcp(127.0.0.1:3306)/mydb?parseTime=true' -X invoice.sql
    open http://127.0.0.1:8080/v1/api/meta/struct/dev/invoice    
```

Provided struct may optionally define the following method to customize fetched/assembled data.

- **OnFetch(ctx context.Context) error**: invoked by reader once record is fetched from database
- **OnRelation(ctx context.Context)**: invoked by reader once all relations are assembled



#### Usage


#### Reading view data 

Basic view reader example
```go
package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	"log"
	"reflect"
	"time"
)

type Invoice struct {
	Id           int32      `sqlx:"id"`
	CustomerName *string    `sqlx:"customer_name"`
	InvoiceDate  *time.Time `sqlx:"invoice_date"`
	DueDate      *time.Time `sqlx:"due_date"`
	TotalAmount  *string    `sqlx:"total_amount"`
	Items        []*Item
}

type Item struct {
	Id          int32   `sqlx:"id"`
	InvoiceId   *int64  `sqlx:"invoice_id"`
	ProductName *string `sqlx:"product_name"`
	Quantity    *int64  `sqlx:"quantity"`
	Price       *string `sqlx:"price"`
	Total       *string `sqlx:"total"`
}




func ExampleService_ReadDataView() {


	aReader := reader.New()
	conn := aReader.Resource.AddConnector("dbName", "database/sql  driverName", "database/sql dsn")

	invoiceView := view.NewView("invoice", "INVOICE",
		view.WithConnector(conn),
		view.WithCriteria("id"),
		view.WithViewType(reflect.TypeOf(&Invoice{})),
		view.WithOneToMany("Items", "id",
			view.NwReferenceView("", "invoice_id",
				view.NewView("items", "invoice_list_item", view.WithConnector(conn)))),
	)

	aReader.Resource.AddViews(invoiceView)
	if err := aReader.Resource.Init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var  invoices= make([]*Invoice, 0)
	if err := aReader.ReadInto(context.Background(), "invoice", &invoices, reader.WithCriteria( "status = ?",1));err != nil {
		log.Fatal(err)
	}
	invociesJSON, _:=json.Marshal(invoices)
	fmt.Printf("invocies: %s\n", invociesJSON)



}

```


#### Reading view data from various databases

A read can read data for one or many database connection while assembling data view.

```go
package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	"log"
	"reflect"
	"time"
)

type Trader struct {
    Id          int32      `sqlx:"id"`
    FirstName   *string    `sqlx:"first_name"`
    LastName    *string    `sqlx:"last_name"`
    Email       *string    `sqlx:"email"`
    PhoneNumber *string    `sqlx:"phone_number"`
    JoinDate    *time.Time `sqlx:"join_date"`
    Acl         *Acl
}

type Acl struct {
    UserId         int   `sqlx:"USER_ID"`
    IsReadonly     *bool `sqlx:"IS_READONLY"`
    CanUseFeature1 *bool `sqlx:"CAN_USE_FEATURE1"`
}


func ExampleService_MultiDbReadInto() {

	aReader := reader.New()
	mainConn := aReader.Resource.AddConnector("main", "mysql", "database/sql dsn")
	aclConn := aReader.Resource.AddConnector("aci", "dynamodb", "database/sql dsn")

	aView := view.NewView("trader", "TRADER",
		view.WithConnector(mainConn),
		view.WithCriteria("id"),
		view.WithViewType(reflect.TypeOf(&Trader{})),
		view.WithOneToOne("Acl", "id",
			view.NwReferenceView("UserId", "USER_ID",
				view.NewView("trader_acl", "USER_ACL",
					view.WithSQL(`SELECT
                          USER_ID,
                          ARRAY_EXISTS(ROLE, 'READ_ONLY') AS IS_READONLY,
                          ARRAY_EXISTS(PERMISSION, 'FEATURE1') AS CAN_USE_FEATURE1
                    FROM USER_ACL `),
					view.WithConnector(aclConn)))),
	)
	aReader.Resource.AddViews(aView)
	if err := aReader.Resource.Init(context.Background()); err != nil {
		log.Fatal(err)
	}

  var traders = make([]*Invoice, 0)
  if err := aReader.ReadInto(context.Background(), "trader", &traders); err != nil {
    log.Fatal(err)
  }
  traderJSON, _ := json.Marshal(traders)
  fmt.Printf("traders: %s\n", traderJSON)

}
```



### Reading data with SQL template

A view can read from a table, SQL or SQL template


```go
package reader_test


type Product struct {
	Id          int32      `sqlx:"ID"`
	Name        *string    `sqlx:"NAME"`
	VendorId    *int64     `sqlx:"VENDOR_ID"`
	Status      *int64     `sqlx:"STATUS"`
	Created     *time.Time `sqlx:"CREATED"`
	UserCreated *int64     `sqlx:"USER_CREATED"`
	Updated     *time.Time `sqlx:"UPDATED"`
	UserUpdated *int64     `sqlx:"USER_UPDATED"`
	Vendor      *Vendor
	Performance []*Performance
}

type Vendor struct {
	Id          int32      `sqlx:"ID"`
	Name        *string    `sqlx:"NAME"`
	AccountId   *int64     `sqlx:"ACCOUNT_ID"`
	Created     *time.Time `sqlx:"CREATED"`
	UserCreated *int64     `sqlx:"USER_CREATED"`
	Updated     *time.Time `sqlx:"UPDATED"`
	UserUpdated *int64     `sqlx:"USER_UPDATED"`
}

type Performance struct {
	LocationId *int     `sqlx:"location_id"`
	ProductId  *int     `sqlx:"product_id"`
	Quantity   *float64 `sqlx:"quantity"`
	Price      *float64 `sqlx:"price"`
}

var perfTemplate = `SELECT
location_id,
product_id,
SUM(quantity) AS quantity,
AVG(payment) * 1.25 AS price
FROM bqdev.product_performance t
WHERE 1 = 1
#if($Unsafe.period == "today")
    AND TIMESTAMP_TRUNC(t.timestamp, DAY) = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
#elseif ($Unsafe.period == "yesterday")
    AND TIMESTAMP_TRUNC(t.timestamp, DAY) = TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 DAY),  DAY)
#end
GROUP BY 1, 2`



func ExampleService_ReadWithTemplate() {


	aReader := reader.New()
	mainConn := aReader.Resource.AddConnector("main", "mysql", "database/sql dsn")
	aclConn := aReader.Resource.AddConnector("perf", "bigquery", "database/sql dsn")


	aView := view.NewView("product", "PRODUCT",
		view.WithConnector(mainConn),
		view.WithCriteria("ID"),
		view.WithViewType(reflect.TypeOf(&Product{})),
		view.WithOneToOne("Vendor", "VENDOR_ID",
			view.NwReferenceView("ID", "ID",
				view.NewView("product_vendor", "VENDOR",
					view.WithConnector(mainConn)))),
		view.WithOneToMany("Performance", "ID",
			view.NwReferenceView("ProductId", "product_id",
				view.NewView("performance", "product_performance",
					view.WithTemplate(view.NewTemplate("performance",
						view.WithTemplateParameter(view.NewParameter("period",
							view.NewQueryLocation("period"),
							view.WithParameterType(reflect.TypeOf("")),
						)))),
					view.WithConnector(aclConn)))),
	)

	aReader.Resource.AddViews(aView)
	if err := aReader.Resource.Init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var products = make([]*Product, 0)

	if err := aReader.ReadInto(context.Background(), "trader", &products, 
		reader.WithParameter("performance:period", "today")
	); err != nil {
		log.Fatal(err)
	}
	productsJSON, _ := json.Marshal(products)
	fmt.Printf("products: %s\n", productsJSON)

}
```

### Reading data with lifecycle hooks

Data view underlying struct has ability to customize final data by implementing the following methods:

- **OnFetch(ctx context.Context) error**: invoked by reader once record is fetched from database
- **OnRelation(ctx context.Context)**: invoked by reader once all relations are assembled


```go
package reader_test

import (
  "context"
  "encoding/json"
  "fmt"
  "github.com/viant/datly/reader"
  "github.com/viant/datly/view"
  "github.com/viant/parsly"
  "github.com/viant/sqlparser"
  "github.com/viant/sqlparser/expr"
  "github.com/viant/sqlparser/node"
  "log"
  "reflect"
 )

type Audience struct {
  Id              int32   `sqlx:"ID"`
  Name            *string `sqlx:"NAME"`
  MatchExpression *string `sqlx:"MATCH_EXPRESSION"`
  dealIds         []int   `sqlx:"-"`
  Deals           []*Deal
}

type Deal struct {
  Id   int32   `sqlx:"ID"`
  Name *string `sqlx:"NAME"`
  Fee  *string `sqlx:"FEE"`
}

func (a *Audience) OnFetch(ctx context.Context) error {
  if a.MatchExpression != nil && *a.MatchExpression != "" {
    qualify := expr.Qualify{}
    cursor := parsly.NewCursor("", []byte(*a.MatchExpression), 0)
    if err := sqlparser.ParseQualify(cursor, &qualify); err != nil {
      return err
    }

    sqlparser.Traverse(qualify.X, func(n node.Node) bool {
      switch actual := n.(type) {
      case *expr.Binary:
        x := sqlparser.Stringify(actual.X)
        if strings.ToLower(actual.Op) == "in" && strings.ToLower(x) == "deals" {
          par := actual.Y.(*expr.Parenthesis)
          values := par.Raw[1 : len(par.Raw)-1]
          for _, value := range strings.Split(values, ",") {
            value = strings.TrimSpace(value)
            if intVal, err := strconv.Atoi(value); err == nil {
              a.dealIds = append(a.dealIds, intVal)
            }
          }
          return true
        }
      }
      return true
    })
  }
  return nil
}

func ExampleService_LifeCycleReadInto() {

  aReader := reader.New()
  mainConn := aReader.Resource.AddConnector("main", "mysql", "database/sql dsn")

  aView := view.NewView("audience", "AUDIENCE",
    view.WithConnector(mainConn),
    view.WithCriteria("ID"),
    view.WithViewType(reflect.TypeOf(&Audience{})),
    view.WithOneToMany("Deals", "deal_ids",
      view.NwReferenceView("ID", "id",
        view.NewView("deal", "DEAL", view.WithConnector(mainConn)))),
  )

  aReader.Resource.AddViews(aView)
  if err := aReader.Resource.Init(context.Background()); err != nil {
    log.Fatal(err)
  }

  var traders = make([]*Audience, 0)
  if err := aReader.ReadInto(context.Background(), "trader", &traders); err != nil {
    log.Fatal(err)
  }
  traderJSON, _ := json.Marshal(traders)
  fmt.Printf("traders: %s\n", traderJSON)
}


```
