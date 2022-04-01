package reader

type Event string

const (
	Pending Event = "Pending"
	Error   Event = "Error"
	Success Event = "Success"
)
