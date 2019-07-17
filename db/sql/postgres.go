package sqldb

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/samwooo/bolsa/cleaner"
)

type PgSQL struct{ *helper }

func (*PgSQL) Name() string { return "POSTGRES" }
func NewPgSQL(host, port, user, password, dbName string) SqlDB {
	db := sqlx.MustConnect("postgres",
		fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s",
			host, port, user, dbName, password))
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	pg := &PgSQL{&helper{db}}
	cleaner.Register(pg)
	return pg
}
