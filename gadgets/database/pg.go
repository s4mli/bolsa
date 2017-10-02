package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Pg struct {
	db *sql.DB
}

func (p *Pg) ensureConnection() *Pg {
	if err := p.db.Ping(); err != nil {
		panic(err)
	}
	return p
}

func (p *Pg) Query(f func(db *sql.DB) error) error {
	p.ensureConnection()
	return f(p.db)
}

func NewPostgres(host string, port int, db, user, password string) *Pg {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable password=%s",
		host, port, user, db, password)
	d, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	d.SetMaxOpenConns(50)
	d.SetMaxIdleConns(50)
	return (&Pg{d}).ensureConnection()
}
