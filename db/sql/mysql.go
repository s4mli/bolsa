package sqldb

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/s4mli/bolsa/cleaner"
)

type MySQL struct{ *helper }

func (*MySQL) Name() string { return "MYSQL" }
func NewMySQL(host, port, user, password, dbName string) SqlDB {
	db := sqlx.MustConnect("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", user, password, host, port, dbName))
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	mysql := &MySQL{&helper{db}}
	cleaner.Register(mysql)
	return mysql

}
