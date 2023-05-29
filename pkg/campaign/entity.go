package main

import (
	"github.com/viant/sqlx/types"
	"reflect"
	"time"
)

var PackageName = "Campaign"

var Types = map[string]reflect.Type{
	"Entity": reflect.TypeOf(Entity{}),
}

type Entity struct {
	Entity []*Campaign `typeName:"Campaign"`
}

type Campaign struct {
	Id                       int                 `sqlx:"name=ID,autoincrement,primaryKey,required"`
	StrId                    *string             `sqlx:"name=STR_ID,unique,table=CI_CAMPAIGN" json:",omitempty"`
	Name                     *string             `sqlx:"name=NAME" json:",omitempty"`
	AdvertiserId             int                 `sqlx:"name=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID,required"`
	IoNumber                 *string             `sqlx:"name=IO_NUMBER" json:",omitempty"`
	IabRating                *string             `sqlx:"name=IAB_RATING" json:",omitempty"`
	IabCat                   *string             `sqlx:"name=IAB_CAT" json:",omitempty"`
	Domain                   *string             `sqlx:"name=DOMAIN" json:",omitempty"`
	PacingType               *string             `sqlx:"name=PACING_TYPE" json:",omitempty"`
	Startdate                *time.Time          `sqlx:"name=STARTDATE" json:",omitempty"`
	Enddate                  *time.Time          `sqlx:"name=ENDDATE" json:",omitempty"`
	LandingPageUrl           *string             `sqlx:"name=LANDING_PAGE_URL" json:",omitempty"`
	CampaignRules            *string             `sqlx:"name=CAMPAIGN_RULES" json:",omitempty"`
	Target                   *string             `sqlx:"name=TARGET" json:",omitempty"`
	Exclusion                *string             `sqlx:"name=EXCLUSION" json:",omitempty"`
	FreqCapping              *float64            `sqlx:"name=FREQ_CAPPING" json:",omitempty"`
	CappingType              *string             `sqlx:"name=CAPPING_TYPE" json:",omitempty"`
	Brand                    *string             `sqlx:"name=BRAND" json:",omitempty"`
	CampaignGoal             *int                `sqlx:"name=CAMPAIGN_GOAL" json:",omitempty"`
	Status                   *int                `sqlx:"name=STATUS" json:",omitempty"`
	ContactName              *string             `sqlx:"name=CONTACT_NAME" json:",omitempty"`
	Phone                    *string             `sqlx:"name=PHONE" json:",omitempty" validate:"omitempty,phone"`
	Email                    *string             `sqlx:"name=EMAIL" json:",omitempty" validate:"omitempty,email"`
	Created                  *time.Time          `sqlx:"name=CREATED" json:",omitempty"`
	Updatetimed              *time.Time          `sqlx:"name=UPDATETIMED" json:",omitempty"`
	CreatedUser              *int                `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID" json:",omitempty"`
	UpdatedUser              *int                `sqlx:"name=UPDATED_USER,refTable=CI_CONTACTS,refColumn=ID" json:",omitempty"`
	MaxBudget                *float64            `sqlx:"name=MAX_BUDGET" json:",omitempty"`
	DailyBudget              *float64            `sqlx:"name=DAILY_BUDGET,generator=0.00" json:",omitempty"`
	MaxBidMultiplier         *float64            `sqlx:"name=MAX_BID_MULTIPLIER" json:",omitempty"`
	MinBidMultiplier         *float64            `sqlx:"name=MIN_BID_MULTIPLIER" json:",omitempty"`
	AgencyCommissionRate     *float64            `sqlx:"name=AGENCY_COMMISSION_RATE" json:",omitempty"`
	LifetimeFreqCap          *float64            `sqlx:"name=LIFETIME_FREQ_CAP,generator=0" json:",omitempty"`
	CommissionRate           *float64            `sqlx:"name=COMMISSION_RATE" json:",omitempty"`
	DataCommissionRate       *float64            `sqlx:"name=DATA_COMMISSION_RATE" json:",omitempty"`
	Archived                 types.BitBool       `sqlx:"name=ARCHIVED,generator=b'0',required"`
	LifetimeImpCap           *int                `sqlx:"name=LIFETIME_IMP_CAP,generator=0" json:",omitempty"`
	DailyImpCap              *int                `sqlx:"name=DAILY_IMP_CAP,generator=0" json:",omitempty"`
	LifetimeClickCap         *int                `sqlx:"name=LIFETIME_CLICK_CAP,generator=0" json:",omitempty"`
	DailyClickCap            *int                `sqlx:"name=DAILY_CLICK_CAP,generator=0" json:",omitempty"`
	EnablePAid               types.BitBool       `sqlx:"name=ENABLE_P_AID,generator=b'0',required"`
	BudgetCompletionStrategy *int                `sqlx:"name=BUDGET_COMPLETION_STRATEGY" json:",omitempty"`
	FreqCapDuration          *int                `sqlx:"name=FREQ_CAP_DURATION" json:",omitempty"`
	FreqCapTimePeriod        *int                `sqlx:"name=FREQ_CAP_TIME_PERIOD,refTable=CI_FREQUENCY_CAP_TIME_PERIOD,refColumn=ID" json:",omitempty"`
	FcapVersion              *int                `sqlx:"name=FCAP_VERSION,generator=2" json:",omitempty"`
	FrequencyCapTypeId       *int                `sqlx:"name=FREQUENCY_CAP_TYPE_ID,generator=1,refTable=CI_FREQUENCY_CAP_TYPE,refColumn=ID" json:",omitempty"`
	ChannelGroupId           int                 `sqlx:"name=CHANNEL_GROUP_ID,generator=1,refTable=CI_CHANNEL_GROUP,refColumn=ID,required"`
	BillableMaxBudget        *float64            `sqlx:"name=BILLABLE_MAX_BUDGET" json:",omitempty"`
	UserIdentifierType       int                 `sqlx:"name=USER_IDENTIFIER_TYPE,generator=1,refTable=CI_USER_IDENTIFIER_TYPE,refColumn=ID,required"`
	PacingMode               int                 `sqlx:"name=PACING_MODE,generator=0,required"`
	IntradayFrontloadPct     float64             `sqlx:"name=INTRADAY_FRONTLOAD_PCT,generator=0,required"`
	ClientPgRate             *float64            `sqlx:"name=CLIENT_PG_RATE" json:",omitempty"`
	ClientNonPgRate          *float64            `sqlx:"name=CLIENT_NON_PG_RATE" json:",omitempty"`
	ManagedRate              *float64            `sqlx:"name=MANAGED_RATE" json:",omitempty"`
	CoManagedFeeType         int                 `sqlx:"name=CO_MANAGED_FEE_TYPE,generator=1,refTable=CI_CO_MANAGED_FEE_TYPE,refColumn=ID,required"`
	Advertiser               *Advertiser         `typeName:"Advertiser" sqlx:"-" datly:"relName=Advertiser,relColumn=ADVERTISER_ID,refColumn=ID,refTable=CI_ADVERTISER" sql:"SELECT av.ID,                            av.CURRENCY_ID,                            DEFAULT_CHANNELS,                            AGENCY_ID,                            (SELECT ctz.IANA_TIMEZONE_STR FROM CI_TIME_ZONE ctz WHERE av.TIME_ZONE_ID = ctz.ID) AS IANA_TIMEZONE                     FROM CI_ADVERTISER av "`
	Flights                  []*Flights          `typeName:"Flights" sqlx:"-" datly:"relName=Flights,relColumn=ID,refColumn=CAMPAIGN_ID,refTable=CI_CAMPAIGN_FLIGHT" sql:"SELECT * FROM CI_CAMPAIGN_FLIGHT"`
	CampaignCreative         []*CampaignCreative `typeName:"CampaignCreative" sqlx:"-" datly:"relName=CampaignCreative,relColumn=ID,refColumn=CAMPAIGN_ID,refTable=CI_CAMPAIGN_CREATIVE" sql:"SELECT * FROM CI_CAMPAIGN_CREATIVE"`
	Creative                 []*Creative         `typeName:"Creative" sqlx:"-" datly:"relName=Creative,relColumn=ID,refColumn=CAMPAIGN_ID,refTable=CI_CREATIVE" sql:"SELECT ID, ADVERTISER_ID, CAMPAIGN_ID FROM CI_CREATIVE"`
	Audience                 []*Audience         `typeName:"Audience" sqlx:"-" datly:"relName=Audience,relColumn=ID,refColumn=CAMPAIGN_ID,refTable=CI_AUDIENCE" sql:"SELECT au.ID, au.TARGET, au.EXCLUSION, ad.CAMPAIGN_ID  FROM CI_AUDIENCE au           JOIN CI_AD_ORDER ad ON au.AD_ORDER_ID = ad.ID"`
	BidOverride              []*BidOverride      `typeName:"BidOverride" sqlx:"-" datly:"relName=BidOverride,relColumn=ID,refColumn=CAMPAIGN_ID,refTable=CI_CAMPAIGN_BID_MULTIPLIER" sql:"SELECT * FROM CI_CAMPAIGN_BID_MULTIPLIER"`
	Event                    *Event              `typeName:"Event" sqlx:"-" datly:"relName=Event,relColumn=ID,refColumn=CAMPAIGN_ID,refTable=CI_EVENT" sql:"SELECT * FROM CI_EVENT"`
	Acl                      *Acl                `typeName:"Acl" sqlx:"-" datly:"relName=Acl,relColumn=CREATED_USER,refColumn=USER_ID,refTable= (CI_CONTACTS) " sql:"     SELECT ID USER_ID,            ACCOUNT_ID,            HasUserRole(ID, 'ROLE_READ_ONLY') AS IS_READ_ONLY,            HasUserRole(ID, 'ROLE_BUSINESS_OWNER') AS IS_BUSINESS_OWNER,            HasUserRole(ID, 'ROLE_ADMIN_CREATION') AS IS_ADMIN_CREATION,            HasUserRole(ID, 'ROLE_ROLES_MANAGEMENT') AS IS_ROLES_MANAGEMENT,            HasUserRole(ID, 'EXPOSE_COMMISSION') AS CAN_EXPOSE_COMMISSION,            HasUserRole(ID, 'ROLE_ADELPHIC_INTERNAL') AS ROLE_ADELPHIC_INTERNAL,            HasAgencyRole(1, ID,  'AGENCY_OWNER') IS_AGENCY_OWNER,            HasAgencyRole(1, ID,  'AGENCY_CAMPAIGN_MEMBER') IS_AGENCY_CAMPAIGN_MEMBER,            HasAdvertiserRole(1, ID,'ADVERTISER_OWNER') IS_ADVERTISER_OWNER,            HasAdvertiserRole(1, ID,'CAMPAIGN_MEMBER') IS_CAMPAIGN_MEMBER     FROM (CI_CONTACTS) "`
	Features                 *Features           `typeName:"Features" sqlx:"-" datly:"relName=Features,relColumn=CREATED_USER,refColumn=USER_ID,refTable= (CI_CONTACTS) " sql:"SELECT                         ID USER_ID,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_DUAL_STATUS') AS DUAL_STATUS,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_HOUSEHOLD_IDENTIFIER') AS HOUSEHOLD_IDENTIFIER,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CAMPAIGN_FLIGHTING') AS CAMPAIGN_FLIGHTING,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CHANNELS_V2') AS EXPOSE_CHANNELS_V2,                         HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_XDEVICE_FREQUENCY_CAPPING') AS XDEVICE_FREQUENCY_CAPPING                     FROM (CI_CONTACTS) "`
	Has                      *CampaignHas        `presenceIndex:"true" typeName:"CampaignHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
}

type Advertiser struct {
	Id              int            `sqlx:"name=ID,primaryKey,required"`
	CurrencyId      int            `sqlx:"name=CURRENCY_ID,refTable=CI_CURRENCY,refColumn=ID,required"`
	DefaultChannels int            `sqlx:"name=DEFAULT_CHANNELS,required"`
	AgencyId        int            `sqlx:"name=AGENCY_ID,refTable=CI_AGENCY,refColumn=ID,required"`
	IanaTimezone    string         `sqlx:"name=IANA_TIMEZONE,required"`
	Has             *AdvertiserHas `presenceIndex:"true" typeName:"AdvertiserHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
}

type AdvertiserHas struct {
	Id              bool
	CurrencyId      bool
	DefaultChannels bool
	AgencyId        bool
	IanaTimezone    bool
}

type Flights struct {
	Id                   int         `sqlx:"name=ID,autoincrement,primaryKey,required"`
	CampaignId           int         `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID,required"`
	StartDate            time.Time   `sqlx:"name=START_DATE,required"`
	EndDate              time.Time   `sqlx:"name=END_DATE,required"`
	DailyBudgetType      int         `sqlx:"name=DAILY_BUDGET_TYPE,refTable=CI_DAILY_BUDGET_TYPE,refColumn=ID,required"`
	DailyBudget          *float64    `sqlx:"name=DAILY_BUDGET" json:",omitempty"`
	LifetimeFrontloadPct float64     `sqlx:"name=LIFETIME_FRONTLOAD_PCT,generator=0,required"`
	DailyImpCap          *int        `sqlx:"name=DAILY_IMP_CAP" json:",omitempty"`
	LifetimeImpCap       *int        `sqlx:"name=LIFETIME_IMP_CAP" json:",omitempty"`
	MaxBudget            float64     `sqlx:"name=MAX_BUDGET,generator=0.00,required"`
	BillableMaxBudget    *float64    `sqlx:"name=BILLABLE_MAX_BUDGET" json:",omitempty"`
	CreatedUser          int         `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID,required"`
	Created              *time.Time  `sqlx:"name=CREATED,generator=CURRENT_TIMESTAMP" json:",omitempty"`
	UpdatedUser          int         `sqlx:"name=UPDATED_USER,refTable=CI_CONTACTS,refColumn=ID,required"`
	Updated              *time.Time  `sqlx:"name=UPDATED,generator=CURRENT_TIMESTAMP" json:",omitempty"`
	Has                  *FlightsHas `presenceIndex:"true" typeName:"FlightsHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
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
	Id         int                  `sqlx:"name=ID,autoincrement,primaryKey,required"`
	CreativeId int                  `sqlx:"name=CREATIVE_ID,refTable=CI_CREATIVE,refColumn=ID,required"`
	CampaignId int                  `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID,required"`
	Created    *time.Time           `sqlx:"name=CREATED,generator=CURRENT_TIMESTAMP" json:",omitempty"`
	Updated    *time.Time           `sqlx:"name=UPDATED,generator=CURRENT_TIMESTAMP" json:",omitempty"`
	Has        *CampaignCreativeHas `presenceIndex:"true" typeName:"CampaignCreativeHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
}

type CampaignCreativeHas struct {
	Id         bool
	CreativeId bool
	CampaignId bool
	Created    bool
	Updated    bool
}

type Creative struct {
	Id           int          `sqlx:"name=ID,primaryKey,required"`
	AdvertiserId int          `sqlx:"name=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID,required"`
	CampaignId   int          `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID,required"`
	Has          *CreativeHas `presenceIndex:"true" typeName:"CreativeHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
}

type CreativeHas struct {
	Id           bool
	AdvertiserId bool
	CampaignId   bool
}

type Audience struct {
	Id         int          `sqlx:"name=ID,primaryKey,required"`
	Target     string       `sqlx:"name=TARGET,required"`
	Exclusion  string       `sqlx:"name=EXCLUSION,required"`
	CampaignId int          `sqlx:"name=CAMPAIGN_ID,required"`
	Has        *AudienceHas `presenceIndex:"true" typeName:"AudienceHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
}

type AudienceHas struct {
	Id         bool
	Target     bool
	Exclusion  bool
	CampaignId bool
}

type BidOverride struct {
	Id            int             `sqlx:"name=ID,autoincrement,primaryKey,required"`
	CampaignId    int             `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID,required"`
	TargetKey     string          `sqlx:"name=TARGET_KEY,required"`
	TargetVal     string          `sqlx:"name=TARGET_VAL,required"`
	BidMultiplier float64         `sqlx:"name=BID_MULTIPLIER,required"`
	CreatedUser   int             `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID,required"`
	Created       time.Time       `sqlx:"name=CREATED,generator=CURRENT_TIMESTAMP,required"`
	UpdatedUser   int             `sqlx:"name=UPDATED_USER,refTable=CI_CONTACTS,refColumn=ID,required"`
	Updatetimed   time.Time       `sqlx:"name=UPDATETIMED,generator=CURRENT_TIMESTAMP,required"`
	Has           *BidOverrideHas `presenceIndex:"true" typeName:"BidOverrideHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
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
	Id           int           `sqlx:"name=ID,autoincrement,primaryKey,required"`
	EventName    string        `sqlx:"name=EVENT_NAME,required"`
	AccountId    int           `sqlx:"name=ACCOUNT_ID,refTable=CI_ACCOUNT,refColumn=ID,required"`
	AgencyId     *int          `sqlx:"name=AGENCY_ID,refTable=CI_AGENCY,refColumn=ID" json:",omitempty"`
	AdvertiserId *int          `sqlx:"name=ADVERTISER_ID,refTable=CI_ADVERTISER,refColumn=ID" json:",omitempty"`
	CampaignId   *int          `sqlx:"name=CAMPAIGN_ID,refTable=CI_CAMPAIGN,refColumn=ID" json:",omitempty"`
	CreativeId   *int          `sqlx:"name=CREATIVE_ID,refTable=CI_CREATIVE,refColumn=ID" json:",omitempty"`
	AdOrderId    *int          `sqlx:"name=AD_ORDER_ID,refTable=CI_AD_ORDER,refColumn=ID" json:",omitempty"`
	CreatedUser  *int          `sqlx:"name=CREATED_USER,refTable=CI_CONTACTS,refColumn=ID" json:",omitempty"`
	Created      *time.Time    `sqlx:"name=CREATED" json:",omitempty"`
	Log          *string       `sqlx:"name=LOG" json:",omitempty"`
	InternalMsg  *string       `sqlx:"name=INTERNAL_MSG" json:",omitempty"`
	Processed    types.BitBool `sqlx:"name=PROCESSED,generator=b'0',required"`
	Has          *EventHas     `presenceIndex:"true" typeName:"EventHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
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
	UserId                 int     `sqlx:"name=USER_ID,required"`
	AccountId              int     `sqlx:"name=ACCOUNT_ID,required"`
	IsReadOnly             int     `sqlx:"name=IS_READ_ONLY,required"`
	IsBusinessOwner        int     `sqlx:"name=IS_BUSINESS_OWNER,required"`
	IsAdminCreation        int     `sqlx:"name=IS_ADMIN_CREATION,required"`
	IsRolesManagement      int     `sqlx:"name=IS_ROLES_MANAGEMENT,required"`
	CanExposeCommission    int     `sqlx:"name=CAN_EXPOSE_COMMISSION,required"`
	RoleAdelphicInternal   int     `sqlx:"name=ROLE_ADELPHIC_INTERNAL,required"`
	IsAgencyOwner          int     `sqlx:"name=IS_AGENCY_OWNER,required"`
	IsAgencyCampaignMember int     `sqlx:"name=IS_AGENCY_CAMPAIGN_MEMBER,required"`
	IsAdvertiserOwner      int     `sqlx:"name=IS_ADVERTISER_OWNER,required"`
	IsCampaignMember       int     `sqlx:"name=IS_CAMPAIGN_MEMBER,required"`
	Has                    *AclHas `presenceIndex:"true" typeName:"AclHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
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
	UserId                  int          `sqlx:"name=USER_ID,required"`
	DualStatus              int          `sqlx:"name=DUAL_STATUS,required"`
	HouseholdIdentifier     int          `sqlx:"name=HOUSEHOLD_IDENTIFIER,required"`
	CampaignFlighting       int          `sqlx:"name=CAMPAIGN_FLIGHTING,required"`
	ExposeChannelsV2        int          `sqlx:"name=EXPOSE_CHANNELS_V2,required"`
	XdeviceFrequencyCapping int          `sqlx:"name=XDEVICE_FREQUENCY_CAPPING,required"`
	Has                     *FeaturesHas `presenceIndex:"true" typeName:"FeaturesHas" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`
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
}
