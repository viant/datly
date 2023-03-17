package types

type (
	Foo struct {
		Id    int
		Name  string
		Price float64
	}

	Boo struct {
		Id        int
		UpdatedAt string
		CreatedAt string
	}

	Bar struct {
		Id    int
		Items []interface{}
	}

	Record struct {
		Id          string
		Preferences interface{} `json:",inline"`
		ClassName   string
	}
)
