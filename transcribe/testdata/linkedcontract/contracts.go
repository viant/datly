package linkedcontract

type Event struct {
	ID   *int64 `json:"id,omitempty" sqlx:"ID,primaryKey=true,autoincrement=true"`
	Name string `json:"name" sqlx:"NAME"`
}

type Input struct {
	Events []*Event `parameter:"Events,kind=body,in=Data,cardinality=Many,required"`
}

type Output struct {
	Data []*Event `parameter:"Data,kind=output,in=view" view:"Events,table=EVENTS" sql:"SELECT ID, NAME FROM EVENTS"`
}

type ComponentInput struct {
	TenantID int `parameter:"TenantID,kind=path,in=tenantID,required"`
}

type ComponentRow struct {
	ID int `sqlx:"id"`
}

type ComponentOutput struct {
	Status string          `parameter:"Status,kind=output,in=status"`
	Data   []*ComponentRow `parameter:"Data,kind=output,in=view" view:"Users,table=users" sql:"SELECT id FROM users"`
}
