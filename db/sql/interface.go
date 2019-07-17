package sqldb

import (
	"github.com/jmoiron/sqlx"
)

type SqlDB interface {
	Query(func(*sqlx.DB) error) error
	QueryTx(func(*sqlx.Tx) error) error
	Close() error
}

type helper struct{ db *sqlx.DB }

func (h *helper) Stop()                                 { h.Close() }
func (h *helper) Close() error                          { return h.db.Close() }
func (h *helper) Query(f func(db *sqlx.DB) error) error { return f(h.db) }
func (h *helper) QueryTx(f func(*sqlx.Tx) error) error {
	tx := h.db.MustBegin()
	return func(err error) error {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			err = tx.Rollback()
		}
		return err
	}(f(tx))
}
