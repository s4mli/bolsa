package database

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Mysql struct {
	db *sql.DB
}

func (m *Mysql) ensureConnection() *Mysql {
	if err := m.db.Ping(); err != nil {
		panic(err)
	}
	return m
}

func (m *Mysql) Query(f func(db *sql.DB) error) error {
	return f(m.ensureConnection().db)
}

func NewMysql(host, port, user, password, db string) *Mysql {
	mySqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8",
		user, password, host, port, db)
	if db, err := sql.Open("mysql", mySqlInfo); err != nil {
		panic(err)
	} else {
		mysql := Mysql{db}
		mysql.ensureConnection()
		return &mysql
	}
}
