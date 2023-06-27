package campaign

import (
	"github.com/viant/sqlx/types"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"reflect"
	"time"
)

var PackageName = "campaign"

func init() {
	core.RegisterType(PackageName, "Campaign", reflect.TypeOf(Campaign{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Advertiser", reflect.TypeOf(Advertiser{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Flights", reflect.TypeOf(Flights{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "CampaignCreative", reflect.TypeOf(CampaignCreative{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Creative", reflect.TypeOf(Creative{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Audience", reflect.TypeOf(Audience{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "BidOverride", reflect.TypeOf(BidOverride{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Event", reflect.TypeOf(Event{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Acl", reflect.TypeOf(Acl{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Features", reflect.TypeOf(Features{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "Handler", reflect.TypeOf(Handler{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "State", reflect.TypeOf(State{}), checksum.GeneratedTime)
}

type Entity struct {
	Entity *Campaign `typeName:"Campaign"`
}

type Campaign struct {
	Id                       int                 `sqlx:"name=ID,autoincrement,primaryKey"`
	StrId                    *string             `sqlx:"name=STR_ID,unique,table=CI_CAMPAIGN" json:",omitempty" validate:"omitempty,le(32)"`
	Name                     *string             `sqlx:"name=NAME" json:",omitempty" validate:"omitempty,le(256)"`
	AdvertiserId             int                 `sqlx:"name=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID" validate:"required"`
	IoNumber                 *string             `sqlx:"name=IO_NUMBER" json:",omitempty" validate:"omitempty,le(128)"`
	IabRating                *string             `sqlx:"name=IAB_RATING" json:",omitempty" validate:"omitempty,le(64)"`
	IabCat                   *string             `sqlx:"name=IAB_CAT" json:",omitempty" validate:"omitempty,le(256)"`
	Domain                   *string             `sqlx:"name=DOMAIN" json:",omitempty" validate:"omitempty,domain,le(256)"`
	PacingType               *string             `sqlx:"name=PACING_TYPE" json:",omitempty" validate:"omitempty,le(64)"`
	Startdate                *time.Time          `sqlx:"name=STARTDATE" json:",omitempty" validate:"omitempty"`
	Enddate                  *time.Time          `sqlx:"name=ENDDATE" json:",omitempty" validate:"omitempty"`
	LandingPageUrl           *string             `sqlx:"name=LANDING_PAGE_URL" json:",omitempty" validate:"omitempty,le(225)"`
	CampaignRules            *string             `sqlx:"name=CAMPAIGN_RULES" json:",omitempty" validate:"omitempty,le(225)"`
	Target                   *string             `sqlx:"name=TARGET" json:",omitempty" validate:"omitempty,le(2048)"`
	Exclusion                *string             `sqlx:"name=EXCLUSION" json:",omitempty" validate:"omitempty,le(2048)"`
	FreqCapping              *float64            `sqlx:"name=FREQ_CAPPING" json:",omitempty" validate:"omitempty"`
	CappingType              *string             `sqlx:"name=CAPPING_TYPE" json:",omitempty" validate:"omitempty,le(32)"`
	Brand                    *string             `sqlx:"name=BRAND" json:",omitempty" validate:"omitempty,le(256)"`
	CampaignGoal             *int                `sqlx:"name=CAMPAIGN_GOAL" json:",omitempty" validate:"omitempty"`
	Status                   *int                `sqlx:"name=STATUS" json:",omitempty" validate:"omitempty"`
	ContactName              *string             `sqlx:"name=CONTACT_NAME" json:",omitempty" validate:"omitempty,le(255)"`
	Phone                    *string             `sqlx:"name=PHONE" json:",omitempty" validate:"omitempty,phone,le(256)"`
	Email                    *string             `sqlx:"name=EMAIL" json:",omitempty" validate:"omitempty,email,le(256)"`
	Created                  *time.Time          `sqlx:"name=CREATED" json:",omitempty" validate:"omitempty"`
	Updatetimed              *time.Time          `sqlx:"name=UPDATETIMED" json:",omitempty" validate:"omitempty"`
	CreatedUser              *int                `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID" json:",omitempty" validate:"omitempty"`
	UpdatedUser              *int                `sqlx:"name=UPDATED_USER,refTable=CI_CONTACTS,refColumn=ID" json:",omitempty" validate:"omitempty"`
	MaxBudget                *float64            `sqlx:"name=MAX_BUDGET" json:",omitempty" validate:"omitempty"`
	DailyBudget              *float64            `sqlx:"name=DAILY_BUDGET" json:",omitempty" validate:"omitempty"`
	MaxBidMultiplier         *float64            `sqlx:"name=MAX_BID_MULTIPLIER" json:",omitempty" validate:"omitempty"`
	MinBidMultiplier         *float64            `sqlx:"name=MIN_BID_MULTIPLIER" json:",omitempty" validate:"omitempty"`
	AgencyCommissionRate     *float64            `sqlx:"name=AGENCY_COMMISSION_RATE" json:",omitempty" validate:"omitempty"`
	LifetimeFreqCap          *float64            `sqlx:"name=LIFETIME_FREQ_CAP" json:",omitempty" validate:"omitempty"`
	CommissionRate           *float64            `sqlx:"name=COMMISSION_RATE" json:",omitempty" validate:"omitempty"`
	DataCommissionRate       *float64            `sqlx:"name=DATA_COMMISSION_RATE" json:",omitempty" validate:"omitempty"`
	Archived                 types.BitBool       `sqlx:"name=ARCHIVED" validate:"required"`
	LifetimeImpCap           *int                `sqlx:"name=LIFETIME_IMP_CAP" json:",omitempty" validate:"omitempty"`
	DailyImpCap              *int                `sqlx:"name=DAILY_IMP_CAP" json:",omitempty" validate:"omitempty"`
	LifetimeClickCap         *int                `sqlx:"name=LIFETIME_CLICK_CAP" json:",omitempty" validate:"omitempty"`
	DailyClickCap            *int                `sqlx:"name=DAILY_CLICK_CAP" json:",omitempty" validate:"omitempty"`
	EnablePAid               types.BitBool       `sqlx:"name=ENABLE_P_AID" validate:"required"`
	BudgetCompletionStrategy *int                `sqlx:"name=BUDGET_COMPLETION_STRATEGY" json:",omitempty" validate:"omitempty"`
	FreqCapDuration          *int                `sqlx:"name=FREQ_CAP_DURATION" json:",omitempty" validate:"omitempty"`
	FreqCapTimePeriod        *int                `sqlx:"name=FREQ_CAP_TIME_PERIOD,refTable=CI_FREQUENCY_CAP_TIME_PERIOD,refColumn=ID" json:",omitempty" validate:"omitempty"`
	FcapVersion              *int                `sqlx:"name=FCAP_VERSION" json:",omitempty" validate:"omitempty"`
	FrequencyCapTypeId       *int                `sqlx:"name=FREQUENCY_CAP_TYPE_ID,refTable=CI_FREQUENCY_CAP_TYPE,refColumn=ID" json:",omitempty" validate:"omitempty"`
	ChannelGroupId           int                 `sqlx:"name=CHANNEL_GROUP_ID,refTable=CI_CHANNEL_GROUP,refColumn=ID" validate:"required"`
	BillableMaxBudget        *float64            `sqlx:"name=BILLABLE_MAX_BUDGET" json:",omitempty" validate:"omitempty"`
	UserIdentifierType       int                 `sqlx:"name=USER_IDENTIFIER_TYPE,refTable=CI_USER_IDENTIFIER_TYPE,refColumn=ID" validate:"required"`
	PacingMode               int                 `sqlx:"name=PACING_MODE" validate:"required"`
	IntradayFrontloadPct     float64             `sqlx:"name=INTRADAY_FRONTLOAD_PCT" validate:"required"`
	ClientPgRate             *float64            `sqlx:"name=CLIENT_PG_RATE" json:",omitempty" validate:"omitempty"`
	ClientNonPgRate          *float64            `sqlx:"name=CLIENT_NON_PG_RATE" json:",omitempty" validate:"omitempty"`
	ManagedRate              *float64            `sqlx:"name=MANAGED_RATE" json:",omitempty" validate:"omitempty"`
	CoManagedFeeType         int                 `sqlx:"name=CO_MANAGED_FEE_TYPE,refTable=CI_CO_MANAGED_FEE_TYPE,refColumn=ID" validate:"required"`
	Advertiser               *Advertiser         `typeName:"Advertiser" sqlx:"-" datly:"ralName=Advertiser,relColumn=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID" sql:"SELECT av.ID,                            av.CURRENCY_ID,                            DEFAULT_CHANNELS,                            AGENCY_ID,                            (SELECT ctz.IANA_TIMEZONE_STR FROM CI_TIME_ZONE ctz WHERE av.TIME_ZONE_ID = ctz.ID) AS IANA_TIMEZONE                     FROM CI_ADVERTISER av "`
	Flights                  []*Flights          `typeName:"Flights" sqlx:"-" datly:"ralName=Flights,relColumn=ID,refTable=CI_CAMPAIGN_FLIGHT,refColumn=CAMPAIGN_ID" sql:"SELECT * FROM CI_CAMPAIGN_FLIGHT"`
	CampaignCreative         []*CampaignCreative `typeName:"CampaignCreative" sqlx:"-" datly:"ralName=CampaignCreative,relColumn=ID,refTable=CI_CAMPAIGN_CREATIVE,refColumn=CAMPAIGN_ID" sql:"SELECT * FROM CI_CAMPAIGN_CREATIVE"`
	Creative                 []*Creative         `typeName:"Creative" sqlx:"-" datly:"ralName=Creative,relColumn=ID,refTable=CI_CREATIVE,refColumn=CAMPAIGN_ID" sql:"SELECT ID, ADVERTISER_ID, CAMPAIGN_ID FROM CI_CREATIVE c"`
	Audience                 []*Audience         `typeName:"Audience" sqlx:"-"`
	BidOverride              []*BidOverride      `typeName:"BidOverride" sqlx:"-" datly:"ralName=BidOverride,relColumn=ID,refTable=CI_CAMPAIGN_BID_MULTIPLIER,refColumn=CAMPAIGN_ID" sql:"SELECT * FROM CI_CAMPAIGN_BID_MULTIPLIER"`
	Event                    *Event              `typeName:"Event" sqlx:"-" datly:"ralName=Event,relColumn=ID,refTable=CI_EVENT,refColumn=CAMPAIGN_ID" sql:"SELECT * FROM CI_EVENT t"`
	Acl                      *Acl                `typeName:"Acl" sqlx:"-" datly:"ralName=Acl,relColumn=CREATED_USER,refTable=CI_CONTACTS,refColumn=USER_ID" sql:"     SELECT ID USER_ID,            ACCOUNT_ID,            HasUserRole(ID, 'ROLE_READ_ONLY') AS IS_READ_ONLY,            HasUserRole(ID, 'ROLE_BUSINESS_OWNER') AS IS_BUSINESS_OWNER,            HasUserRole(ID, 'ROLE_ADMIN_CREATION') AS IS_ADMIN_CREATION,            HasUserRole(ID, 'ROLE_ROLES_MANAGEMENT') AS IS_ROLES_MANAGEMENT,            HasUserRole(ID, 'EXPOSE_COMMISSION') AS CAN_EXPOSE_COMMISSION,            HasUserRole(ID, 'ROLE_ADELPHIC_INTERNAL') AS ROLE_ADELPHIC_INTERNAL,            HasAgencyRole(1, ID,  'AGENCY_OWNER') IS_AGENCY_OWNER,            HasAgencyRole(1, ID,  'AGENCY_CAMPAIGN_MEMBER') IS_AGENCY_CAMPAIGN_MEMBER,            HasAdvertiserRole(1, ID,'ADVERTISER_OWNER') IS_ADVERTISER_OWNER,            HasAdvertiserRole(1, ID,'CAMPAIGN_MEMBER') IS_CAMPAIGN_MEMBER     FROM CI_CONTACTS "`
	Features                 *Features           `typeName:"Features" sqlx:"-" datly:"ralName=Features,relColumn=CREATED_USER,refTable=CI_CONTACTS,refColumn=USER_ID" sql:"SELECT                         ID USER_ID,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_DUAL_STATUS') AS DUAL_STATUS,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_HOUSEHOLD_IDENTIFIER') AS HOUSEHOLD_IDENTIFIER,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CAMPAIGN_FLIGHTING') AS CAMPAIGN_FLIGHTING,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CHANNELS_V2') AS EXPOSE_CHANNELS_V2,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_XDEVICE_FREQUENCY_CAPPING') AS XDEVICE_FREQUENCY_CAPPING                     FROM CI_CONTACTS "`
	Has                      *CampaignHas        `setMarker:"true" typeName:"CampaignHas" json:"-"  sqlx:"-" `
}

type Advertiser struct {
	Id              int            `sqlx:"name=ID,autoincrement,primaryKey"`
	CurrencyId      int            `sqlx:"name=CURRENCY_ID,refTable=CI_CURRENCY,refColumn=ID" validate:"required"`
	DefaultChannels *int           `sqlx:"name=DEFAULT_CHANNELS" json:",omitempty" validate:"omitempty"`
	AgencyId        *int           `sqlx:"name=AGENCY_ID,refTable=CI_AGENCY,refColumn=ID" json:",omitempty" validate:"omitempty"`
	IanaTimezone    *string        `sqlx:"name=IANA_TIMEZONE" json:",omitempty" validate:"omitempty"`
	Has             *AdvertiserHas `setMarker:"true" typeName:"AdvertiserHas" json:"-"  sqlx:"-" `
}

type AdvertiserHas struct {
	Id              bool
	CurrencyId      bool
	DefaultChannels bool
	AgencyId        bool
	IanaTimezone    bool
}

type Flights struct {
	Id                   int         `sqlx:"name=ID,autoincrement,primaryKey"`
	CampaignId           int         `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID" validate:"required"`
	StartDate            time.Time   `sqlx:"name=START_DATE" validate:"required"`
	EndDate              time.Time   `sqlx:"name=END_DATE" validate:"required"`
	DailyBudgetType      int         `sqlx:"name=DAILY_BUDGET_TYPE,refTable=CI_DAILY_BUDGET_TYPE,refColumn=ID" validate:"required"`
	DailyBudget          *float64    `sqlx:"name=DAILY_BUDGET" json:",omitempty" validate:"omitempty"`
	LifetimeFrontloadPct float64     `sqlx:"name=LIFETIME_FRONTLOAD_PCT" validate:"required"`
	DailyImpCap          *int        `sqlx:"name=DAILY_IMP_CAP" json:",omitempty" validate:"omitempty"`
	LifetimeImpCap       *int        `sqlx:"name=LIFETIME_IMP_CAP" json:",omitempty" validate:"omitempty"`
	MaxBudget            float64     `sqlx:"name=MAX_BUDGET" validate:"required"`
	BillableMaxBudget    *float64    `sqlx:"name=BILLABLE_MAX_BUDGET" json:",omitempty" validate:"omitempty"`
	CreatedUser          int         `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID" validate:"required"`
	Created              *time.Time  `sqlx:"name=CREATED" json:",omitempty" validate:"omitempty"`
	UpdatedUser          int         `sqlx:"name=UPDATED_USER,refTable=CI_CONTACTS,refColumn=ID" validate:"required"`
	Updated              *time.Time  `sqlx:"name=UPDATED" json:",omitempty" validate:"omitempty"`
	Has                  *FlightsHas `setMarker:"true" typeName:"FlightsHas" json:"-"  sqlx:"-" `
}

type FlightsHas struct {
	Id                   bool
	CampaignId           bool
	StartDate            bool
	EndDate              bool
	DailyBudgetType      bool
	DailyBudget          bool
	LifetimeFrontloadPct bool
	DailyImpCap          bool
	LifetimeImpCap       bool
	MaxBudget            bool
	BillableMaxBudget    bool
	CreatedUser          bool
	Created              bool
	UpdatedUser          bool
	Updated              bool
}

type CampaignCreative struct {
	Id         int                  `sqlx:"name=ID,autoincrement,primaryKey"`
	CreativeId int                  `sqlx:"name=CREATIVE_ID,refTable=CI_CREATIVE,refColumn=ID" validate:"required"`
	CampaignId int                  `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID" validate:"required"`
	Created    *time.Time           `sqlx:"name=CREATED" json:",omitempty" validate:"omitempty"`
	Updated    *time.Time           `sqlx:"name=UPDATED" json:",omitempty" validate:"omitempty"`
	Has        *CampaignCreativeHas `setMarker:"true" typeName:"CampaignCreativeHas" json:"-"  sqlx:"-" `
}

type CampaignCreativeHas struct {
	Id         bool
	CreativeId bool
	CampaignId bool
	Created    bool
	Updated    bool
}

type Creative struct {
	Id           int          `sqlx:"name=ID,autoincrement,primaryKey"`
	CampaignId   *int         `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID" json:",omitempty" validate:"omitempty"`
	AdvertiserId int          `sqlx:"name=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID" validate:"required"`
	Has          *CreativeHas `setMarker:"true" typeName:"CreativeHas" json:"-"  sqlx:"-" `
}

type CreativeHas struct {
	Id           bool
	CampaignId   bool
	AdvertiserId bool
}

type Audience struct {
	Id        int          `sqlx:"name=ID,autoincrement,primaryKey"`
	Target    *string      `sqlx:"name=TARGET" json:",omitempty" validate:"omitempty,le(65535)"`
	Exclusion *string      `sqlx:"name=EXCLUSION" json:",omitempty" validate:"omitempty,le(65535)"`
	Has       *AudienceHas `setMarker:"true" typeName:"AudienceHas" json:"-"  sqlx:"-" `
}

type AudienceHas struct {
	Id        bool
	Target    bool
	Exclusion bool
}

type BidOverride struct {
	Id            int             `sqlx:"name=ID,autoincrement,primaryKey"`
	CampaignId    int             `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID" validate:"required"`
	TargetKey     string          `sqlx:"name=TARGET_KEY" validate:"required,le(96)"`
	TargetVal     string          `sqlx:"name=TARGET_VAL" validate:"required,le(256)"`
	BidMultiplier float64         `sqlx:"name=BID_MULTIPLIER" validate:"required"`
	CreatedUser   int             `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID" validate:"required"`
	Created       time.Time       `sqlx:"name=CREATED" validate:"required"`
	UpdatedUser   int             `sqlx:"name=UPDATED_USER,refTable=CI_CONTACTS,refColumn=ID" validate:"required"`
	Updatetimed   time.Time       `sqlx:"name=UPDATETIMED" validate:"required"`
	Has           *BidOverrideHas `setMarker:"true" typeName:"BidOverrideHas" json:"-"  sqlx:"-" `
}

type BidOverrideHas struct {
	Id            bool
	CampaignId    bool
	TargetKey     bool
	TargetVal     bool
	BidMultiplier bool
	CreatedUser   bool
	Created       bool
	UpdatedUser   bool
	Updatetimed   bool
}

type Event struct {
	Id           int           `sqlx:"name=ID,autoincrement,primaryKey"`
	EventName    string        `sqlx:"name=EVENT_NAME" validate:"required,le(64)"`
	AccountId    int           `sqlx:"name=ACCOUNT_ID,refTable=CI_ACCOUNT,refColumn=ID" validate:"required"`
	AgencyId     *int          `sqlx:"name=AGENCY_ID,refTable=CI_AGENCY,refColumn=ID" json:",omitempty" validate:"omitempty"`
	AdvertiserId *int          `sqlx:"name=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID" json:",omitempty" validate:"omitempty"`
	CampaignId   *int          `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID" json:",omitempty" validate:"omitempty"`
	CreativeId   *int          `sqlx:"name=CREATIVE_ID,refTable=CI_CREATIVE,refColumn=ID" json:",omitempty" validate:"omitempty"`
	AdOrderId    *int          `sqlx:"name=AD_ORDER_ID,refTable=CI_AD_ORDER,refColumn=ID" json:",omitempty" validate:"omitempty"`
	CreatedUser  *int          `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID" json:",omitempty" validate:"omitempty"`
	Created      *time.Time    `sqlx:"name=CREATED" json:",omitempty" validate:"omitempty"`
	Log          *string       `sqlx:"name=LOG" json:",omitempty" validate:"omitempty,le(4294967295)"`
	InternalMsg  *string       `sqlx:"name=INTERNAL_MSG" json:",omitempty" validate:"omitempty,le(8192)"`
	Processed    types.BitBool `sqlx:"name=PROCESSED" validate:"required"`
	Has          *EventHas     `setMarker:"true" typeName:"EventHas" json:"-"  sqlx:"-" `
}

type EventHas struct {
	Id           bool
	EventName    bool
	AccountId    bool
	AgencyId     bool
	AdvertiserId bool
	CampaignId   bool
	CreativeId   bool
	AdOrderId    bool
	CreatedUser  bool
	Created      bool
	Log          bool
	InternalMsg  bool
	Processed    bool
}

type Acl struct {
	UserId                 int     `sqlx:"name=USER_ID" validate:"required"`
	AccountId              int     `sqlx:"name=ACCOUNT_ID,refTable=CI_ACCOUNT,refColumn=ID" validate:"required"`
	IsReadOnly             *int    `sqlx:"name=IS_READ_ONLY" json:",omitempty" validate:"omitempty"`
	IsBusinessOwner        *int    `sqlx:"name=IS_BUSINESS_OWNER" json:",omitempty" validate:"omitempty"`
	IsAdminCreation        *int    `sqlx:"name=IS_ADMIN_CREATION" json:",omitempty" validate:"omitempty"`
	IsRolesManagement      *int    `sqlx:"name=IS_ROLES_MANAGEMENT" json:",omitempty" validate:"omitempty"`
	CanExposeCommission    *int    `sqlx:"name=CAN_EXPOSE_COMMISSION" json:",omitempty" validate:"omitempty"`
	RoleAdelphicInternal   *int    `sqlx:"name=ROLE_ADELPHIC_INTERNAL" json:",omitempty" validate:"omitempty"`
	IsAgencyOwner          *int    `sqlx:"name=IS_AGENCY_OWNER" json:",omitempty" validate:"omitempty"`
	IsAgencyCampaignMember *int    `sqlx:"name=IS_AGENCY_CAMPAIGN_MEMBER" json:",omitempty" validate:"omitempty"`
	IsAdvertiserOwner      *int    `sqlx:"name=IS_ADVERTISER_OWNER" json:",omitempty" validate:"omitempty"`
	IsCampaignMember       *int    `sqlx:"name=IS_CAMPAIGN_MEMBER" json:",omitempty" validate:"omitempty"`
	Has                    *AclHas `setMarker:"true" typeName:"AclHas" json:"-"  sqlx:"-" `
}

type AclHas struct {
	UserId                 bool
	AccountId              bool
	IsReadOnly             bool
	IsBusinessOwner        bool
	IsAdminCreation        bool
	IsRolesManagement      bool
	CanExposeCommission    bool
	RoleAdelphicInternal   bool
	IsAgencyOwner          bool
	IsAgencyCampaignMember bool
	IsAdvertiserOwner      bool
	IsCampaignMember       bool
}

type Features struct {
	UserId                  int          `sqlx:"name=USER_ID" validate:"required"`
	DualStatus              *int         `sqlx:"name=DUAL_STATUS" json:",omitempty" validate:"omitempty"`
	HouseholdIdentifier     *int         `sqlx:"name=HOUSEHOLD_IDENTIFIER" json:",omitempty" validate:"omitempty"`
	CampaignFlighting       *int         `sqlx:"name=CAMPAIGN_FLIGHTING" json:",omitempty" validate:"omitempty"`
	ExposeChannelsV2        *int         `sqlx:"name=EXPOSE_CHANNELS_V2" json:",omitempty" validate:"omitempty"`
	XdeviceFrequencyCapping *int         `sqlx:"name=XDEVICE_FREQUENCY_CAPPING" json:",omitempty" validate:"omitempty"`
	Has                     *FeaturesHas `setMarker:"true" typeName:"FeaturesHas" json:"-"  sqlx:"-" `
}

type FeaturesHas struct {
	UserId                  bool
	DualStatus              bool
	HouseholdIdentifier     bool
	CampaignFlighting       bool
	ExposeChannelsV2        bool
	XdeviceFrequencyCapping bool
}

type CampaignHas struct {
	Id                       bool
	StrId                    bool
	Name                     bool
	AdvertiserId             bool
	IoNumber                 bool
	IabRating                bool
	IabCat                   bool
	Domain                   bool
	PacingType               bool
	Startdate                bool
	Enddate                  bool
	LandingPageUrl           bool
	CampaignRules            bool
	Target                   bool
	Exclusion                bool
	FreqCapping              bool
	CappingType              bool
	Brand                    bool
	CampaignGoal             bool
	Status                   bool
	ContactName              bool
	Phone                    bool
	Email                    bool
	Created                  bool
	Updatetimed              bool
	CreatedUser              bool
	UpdatedUser              bool
	MaxBudget                bool
	DailyBudget              bool
	MaxBidMultiplier         bool
	MinBidMultiplier         bool
	AgencyCommissionRate     bool
	LifetimeFreqCap          bool
	CommissionRate           bool
	DataCommissionRate       bool
	Archived                 bool
	LifetimeImpCap           bool
	DailyImpCap              bool
	LifetimeClickCap         bool
	DailyClickCap            bool
	EnablePAid               bool
	BudgetCompletionStrategy bool
	FreqCapDuration          bool
	FreqCapTimePeriod        bool
	FcapVersion              bool
	FrequencyCapTypeId       bool
	ChannelGroupId           bool
	BillableMaxBudget        bool
	UserIdentifierType       bool
	PacingMode               bool
	IntradayFrontloadPct     bool
	ClientPgRate             bool
	ClientNonPgRate          bool
	ManagedRate              bool
	CoManagedFeeType         bool
	Advertiser               bool
	Flights                  bool
	CampaignCreative         bool
	Creative                 bool
	Audience                 bool
	BidOverride              bool
	Event                    bool
	Acl                      bool
	Features                 bool
}
