package comprehensive

// Event is the package-authoritative entity used by the comprehensive-many
// transcription fixture.
type Event struct {
	Id   *int64 `json:"id,omitempty" sqlx:"ID,primaryKey=true,autoincrement=true"`
	Name string `json:"name" sqlx:"NAME"`
}
