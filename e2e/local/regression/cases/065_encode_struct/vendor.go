package _65_encode_struct

type VendorIds struct {
	ID          int `criteria:"column=ID"`
	AccountID   int `criteria:"column=ACCOUNT_ID"`
	UserCreated int `criteria:"column=USER_CREATED"`
}
