package sqlnormalizer

type Case struct {
	Name      string
	Generated bool
	SQL       string
	Expect    string
}

func Cases() []Case {
	return []Case{
		{
			Name:      "skip normalization when not generated",
			Generated: false,
			SQL:       "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id",
			Expect:    "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id",
		},
		{
			Name:      "invalid sql returns input",
			Generated: true,
			SQL:       "SELECT * FROM (",
			Expect:    "SELECT * FROM (",
		},
		{
			Name:      "normalize from and join aliases in selectors and alias nodes",
			Generated: true,
			SQL:       "SELECT a.id, b.user_id FROM users a JOIN orders b ON a.id = b.user_id",
			Expect:    "SELECT A.id, B.user_id FROM users A JOIN orders B ON A.id = B.user_id",
		},
		{
			Name:      "keep alias that is already normalized",
			Generated: true,
			SQL:       "SELECT UserAlias.id FROM users UserAlias",
			Expect:    "SELECT UserAlias.id FROM users UserAlias",
		},
		{
			Name:      "normalize snake_case alias",
			Generated: true,
			SQL:       "SELECT order_item.id FROM users order_item",
			Expect:    "SELECT OrderItem.id FROM users OrderItem",
		},
	}
}
