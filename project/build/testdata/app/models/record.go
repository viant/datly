package models

type Record struct {
	ID   int    `sqlx:"id,primaryKey=true" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
