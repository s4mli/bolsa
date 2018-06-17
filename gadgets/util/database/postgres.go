package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Postgres struct {
	db *sql.DB
}

func (p *Postgres) ensureConnection() *Postgres {
	if err := p.db.Ping(); err != nil {
		panic(err)
	}
	return p
}

func (p *Postgres) Query(f func(db *sql.DB) error) error {
	return f(p.ensureConnection().db)
}

func NewPostgres(host, port, db, user, password string) *Postgres {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s",
		host, port, user, db, password)
	if d, err := sql.Open("postgres", psqlInfo); err != nil {
		panic(err)
	} else {
		d.SetMaxOpenConns(50)
		d.SetMaxIdleConns(50)
		return (&Postgres{d}).ensureConnection()
	}
}
