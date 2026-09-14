// Package reader contains a package-authored Datly 1.0 reader example.
package reader

import (
	"time"

	"github.com/viant/scy/auth/jwt"
	xdatly "github.com/viant/xdatly"
)

// Record is the direct RECORDS projection used by the example reader.
type Record struct {
	ID               int        `json:"id" sqlx:"ID,primaryKey=true"`
	Name             string     `json:"name" sqlx:"NAME"`
	ParentID         *int       `json:"parentId,omitempty" sqlx:"PARENT_ID"`
	Target           *string    `json:"target,omitempty" sqlx:"TARGET"`
	Exclusion        *string    `json:"exclusion,omitempty" sqlx:"EXCLUSION"`
	UnitPrice        *float64   `json:"unitPrice,omitempty" sqlx:"UNIT_PRICE"`
	Weight           int        `json:"weight" sqlx:"WEIGHT"`
	Status           int        `json:"status" sqlx:"STATUS"`
	RateLimit        *float64   `json:"rateLimit,omitempty" sqlx:"RATE_LIMIT"`
	RateDuration     *int       `json:"rateDuration,omitempty" sqlx:"RATE_DURATION"`
	RateWindow       *int       `json:"rateWindow,omitempty" sqlx:"RATE_WINDOW"`
	LifetimeLimit    float64    `json:"lifetimeLimit" sqlx:"LIFETIME_LIMIT"`
	ExternalRecordID *string    `json:"externalRecordId,omitempty" sqlx:"EXTERNAL_RECORD_ID"`
	CapacityLimit    *int       `json:"capacityLimit,omitempty" sqlx:"CAPACITY_LIMIT"`
	ManualAssignment int        `json:"manualAssignment" sqlx:"MANUAL_ASSIGNMENT"`
	IsPriority       int        `json:"isPriority" sqlx:"IS_PRIORITY"`
	Created          *time.Time `json:"created,omitempty" sqlx:"CREATED"`
	CreatedUser      *int       `json:"createdUser,omitempty" sqlx:"CREATED_USER"`
	Updated          *time.Time `json:"updated,omitempty" sqlx:"UPDATED"`
	UpdatedUser      *int       `json:"updatedUser,omitempty" sqlx:"UPDATED_USER"`
}

// AuthOutput is the part of the ACL component output consumed by this example.
type AuthOutput struct {
	Allowed bool `parameter:"Allowed,kind=output,in=allowed"`
}

// RecordInput demonstrates transport, codec, component, predicate, and query
// selector bindings on one reader contract.
type RecordInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,errorCode=401" codec:"JwtClaim"`

	ID      []int  `parameter:"ID,kind=path,in=id,cardinality=Many" predicate:"in,group=0,a,ID"`
	GroupID []int  `parameter:"GroupID,kind=query,in=groupId,cardinality=Many" predicate:"in,group=0,c,ID"`
	OrderID []int  `parameter:"OrderID,kind=query,in=orderId,cardinality=Many" predicate:"in,group=0,ao,ID"`
	Name    string `parameter:"Name,kind=query,in=name" predicate:"equal,group=0,a,NAME"`

	IncludeArchived bool `parameter:"IncludeArchived,kind=query,in=includeArchivedRecords"`

	Auth *AuthOutput `parameter:"Auth,kind=component,in=GET:/v1/api/platform/acl/auth,required" predicate:"handler,group=0,*authorization.Record"`

	Fields  []string `parameter:"Fields,kind=query,in=fields" querySelector:"view=Record"`
	OrderBy string   `parameter:"OrderBy,kind=query,in=orderBy" querySelector:"view=Record"`
	Limit   int      `parameter:"Limit,kind=query,in=limit" querySelector:"view=Record"`

	Has *RecordInputHas `setMarker:"true" json:"-" sqlx:"-"`
}

type RecordInputHas struct {
	JWT             bool
	ID              bool
	GroupID         bool
	OrderID         bool
	Name            bool
	IncludeArchived bool
	Auth            bool
	Fields          bool
	OrderBy         bool
	Limit           bool
}

type RecordOutput struct {
	Status string `json:"status" parameter:"Status,kind=output,in=status"`

	Data []*Record `json:"data" parameter:"Data,kind=output,in=view,cardinality=Many" view:"Record,type=Record,table=RECORDS,connector=main,allowNulls=true,batch=10000,selectorNamespace=a,selectorProjection=true,selectorOrderBy=true,selectorCriteria=true,selectorLimit=true" sql:"SELECT a.ID, a.NAME, a.PARENT_ID, a.TARGET, a.EXCLUSION, a.UNIT_PRICE, a.WEIGHT, a.STATUS, a.RATE_LIMIT, a.RATE_DURATION, a.RATE_WINDOW, a.LIFETIME_LIMIT, a.EXTERNAL_RECORD_ID, a.CAPACITY_LIMIT, a.MANUAL_ASSIGNMENT, a.IS_PRIORITY, a.CREATED, a.CREATED_USER, a.UPDATED, a.UPDATED_USER FROM RECORDS a ${predicate.Builder().CombineOr($predicate.FilterGroup(0, \"AND\")).Build(\"WHERE\")}"`
}

// Components is the Datly 1.0 package-level component declaration.
type Components struct {
	Record xdatly.Component[RecordInput, RecordOutput] `component:"Record,path=/v1/api/records/{id},method=GET,connector=main,view=Record" desc:"Reads RECORDS records"`
}
