package sqldb

import (
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"github.com/samwooo/bolsa/cleaner"
)

type MsSQL struct{ *helper }

func (*MsSQL) Name() string { return "MSSQL" }
func NewMsSQL(host, port, user, password, dbName string) SqlDB {
	db := sqlx.MustConnect("mssql",
		fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s", host, user, password, port, dbName))
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	m := &MsSQL{&helper{db}}
	cleaner.Register(m)
	return m

}
