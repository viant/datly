package case001

import "time"

type Foo struct {
	Name     string
	ID       int
	Price    float64
	Boo      Boo
	BooPtr   *Boo
	BooSlice []Boo
	BooMap   map[string]Boo
}

type Boo struct {
	BooID     int
	BooName   string
	CreatedAt time.Time
}
