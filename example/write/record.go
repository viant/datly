// Package write contains a package-authored Datly 1.0 writer example.
package write

import (
	"time"

	"github.com/viant/scy/auth/jwt"
	xdatly "github.com/viant/xdatly"
)

// Record is the writable RECORDS record.
type Record struct {
	ID               int        `json:"id,omitempty" sqlx:"ID,primaryKey=true,autoincrement=true"`
	ParentID         *int       `json:"parentId,omitempty" sqlx:"PARENT_ID"`
	Name             string     `json:"name,omitempty" sqlx:"NAME"`
	Target           *string    `json:"target,omitempty" sqlx:"TARGET"`
	Exclusion        *string    `json:"exclusion,omitempty" sqlx:"EXCLUSION"`
	UnitPrice        *float64   `json:"unitPrice,omitempty" sqlx:"UNIT_PRICE"`
	Weight           int        `json:"weight,omitempty" sqlx:"WEIGHT"`
	Status           int        `json:"status,omitempty" sqlx:"STATUS"`
	RateLimit        *float64   `json:"rateLimit,omitempty" sqlx:"RATE_LIMIT"`
	RateDuration     *int       `json:"rateDuration,omitempty" sqlx:"RATE_DURATION"`
	RateWindow       *int       `json:"rateWindow,omitempty" sqlx:"RATE_WINDOW"`
	LifetimeLimit    float64    `json:"lifetimeLimit,omitempty" sqlx:"LIFETIME_LIMIT"`
	ExternalRecordID *string    `json:"externalRecordId,omitempty" sqlx:"EXTERNAL_RECORD_ID"`
	CapacityLimit    *int       `json:"capacityLimit,omitempty" sqlx:"CAPACITY_LIMIT"`
	ManualAssignment int        `json:"manualAssignment,omitempty" sqlx:"MANUAL_ASSIGNMENT"`
	IsPriority       int        `json:"isPriority,omitempty" sqlx:"IS_PRIORITY"`
	Created          *time.Time `json:"-" sqlx:"CREATED"`
	CreatedUser      *int       `json:"-" sqlx:"CREATED_USER"`
	Updated          *time.Time `json:"-" sqlx:"UPDATED"`
	UpdatedUser      *int       `json:"-" sqlx:"UPDATED_USER"`

	Has *RecordHas `json:"-" sqlx:"-" setMarker:"true" typeName:"RecordHas"`
}

// RecordHas distinguishes an absent PATCH property from an explicit zero or
// null value.
type RecordHas struct {
	ID               bool
	ParentID         bool
	Name             bool
	Target           bool
	Exclusion        bool
	UnitPrice        bool
	Weight           bool
	Status           bool
	RateLimit        bool
	RateDuration     bool
	RateWindow       bool
	LifetimeLimit    bool
	ExternalRecordID bool
	CapacityLimit    bool
	ManualAssignment bool
	IsPriority       bool
	Created          bool
	CreatedUser      bool
	Updated          bool
	UpdatedUser      bool
}

type AuthOutput struct {
	Allowed bool `parameter:"Allowed,kind=output,in=allowed"`
}

type IDSet struct {
	Values []int
}

// RecordInput demonstrates the complete writer binding chain: request
// metadata, a child component, request body, derived parameters, and current
// database state.
type RecordInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,errorCode=401" codec:"JwtClaim"`

	Auth *AuthOutput `parameter:"Auth,kind=component,in=GET:/v1/api/platform/acl/auth,required"`

	Method      string    `parameter:"Method,kind=http_request,in=method"`
	RequestTime time.Time `parameter:"RequestTime,kind=generator,in=current_time"`
	Source      string    `parameter:"Source,kind=query,in=source"`

	Records []*Record `parameter:"Records,kind=body,in=data,cardinality=Many,required"`

	RecordIDs *IDSet `parameter:"RecordIDs,kind=param,in=Records" codec:"structql,uri=record/cur_record_ids.sql"`

	CurrentRecords []*Record `parameter:"CurrentRecords,kind=view,in=CurrentRecords,cardinality=Many" view:"CurrentRecords,type=Record,table=RECORDS,connector=main,limit=0" sql:"SELECT au.ID, au.PARENT_ID, au.NAME, au.TARGET, au.EXCLUSION, au.UNIT_PRICE, au.WEIGHT, au.STATUS, au.RATE_LIMIT, au.RATE_DURATION, au.RATE_WINDOW, au.LIFETIME_LIMIT, au.EXTERNAL_RECORD_ID, au.CAPACITY_LIMIT, au.MANUAL_ASSIGNMENT, au.IS_PRIORITY, au.CREATED, au.CREATED_USER, au.UPDATED, au.UPDATED_USER FROM RECORDS au WHERE $criteria.In(\"ID\", $RecordIDs.Values)"`
}

type RecordOutput struct {
	Status string `json:"status" parameter:"Status,kind=output,in=status"`

	Data []*Record `json:"data,omitempty" parameter:"Data,kind=output,in=view,cardinality=Many" view:"Record,type=Record,table=RECORDS,connector=main"`

	Violations []string `json:"violations,omitempty" parameter:"Violations,kind=output,in=violations"`
}

// Components is the Datly 1.0 package-level component declaration. The named
// handler owns record validation and DML orchestration.
type Components struct {
	RecordPatch xdatly.Component[RecordInput, RecordOutput] `component:"RecordPatch,path=/v1/api/records,method=PATCH,connector=main,handler=NewRecordPatchHandler,view=Record" desc:"Inserts and updates RECORDS records"`
}
