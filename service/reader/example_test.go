package reader_test

/*
import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"log"
	"reflect"
	"strconv"
	"strings"
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

func ExampleService_ReadInto() {

	aReader := reader.New()
	conn := aReader.Resource.AddConnector("dbName", "database/sql  driverName", "database/sql dsn")

	invoiceView := view.NewView("invoice", "INVOICE",
		view.WithConnector(conn),
		view.WithCriteria("id"),
		view.WithViewType(reflect.TypeOf(&Invoice{})),
		view.WithOneToMany("items", "id",
			view.NwReferenceView("", "invoice_id",
				view.NewView("items", "invoice_list_item", view.WithConnector(conn)))),
	)

	aReader.Resource.AddViews(invoiceView)
	if err := aReader.Resource.init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var invoices = make([]*Invoice, 0)
	if err := aReader.ReadInto(context.Background(), "invoice", &invoices, reader.WithCriteria("status = ?", 1)); err != nil {
		log.Fatal(err)
	}
	invoiceJSON, _ := json.Marshal(invoices)
	fmt.Printf("invocies: %s\n", invoiceJSON)

}

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
	if err := aReader.Resource.init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var traders = make([]*Invoice, 0)
	if err := aReader.ReadInto(context.Background(), "trader", &traders); err != nil {
		log.Fatal(err)
	}
	traderJSON, _ := json.Marshal(traders)
	fmt.Printf("traders: %s\n", traderJSON)
}

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
						view.WithTemplateParameter(state.NewParameter("period",
							state.NewQueryLocation("period"),
							state.WithParameterType(reflect.TypeOf("")),
						)))),
					view.WithConnector(aclConn)))),
	)

	aReader.Resource.AddViews(aView)
	if err := aReader.Resource.init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var products = make([]*Product, 0)

	if err := aReader.ReadInto(context.Background(), "trader", &products,
		reader.WithParameter("performance:period", "today")); err != nil {
		log.Fatal(err)
	}
	productsJSON, _ := json.Marshal(products)
	fmt.Printf("products: %s\n", productsJSON)

}

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
	if err := aReader.Resource.init(context.Background()); err != nil {
		log.Fatal(err)
	}

	var traders = make([]*Audience, 0)
	if err := aReader.ReadInto(context.Background(), "trader", &traders); err != nil {
		log.Fatal(err)
	}
	traderJSON, _ := json.Marshal(traders)
	fmt.Printf("traders: %s\n", traderJSON)
}


*/
