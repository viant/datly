package tag

type Tag struct {
	Name      string
	Table     string
	SQL       string
	Column    string
	Transient bool
	Alias     string
	On        string
}
