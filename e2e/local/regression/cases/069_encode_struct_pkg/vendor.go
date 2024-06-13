package encode_struct_pkg

type VendorIds struct {
	ID          int `sqlx:"column=ID"`
	AccountID   int `sqlx:"column=ACCOUNT_ID"`
	UserCreated int `sqlx:"column=USER_CREATED"`
}
