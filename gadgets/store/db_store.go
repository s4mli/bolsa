package store

type DBStore struct{}

func NewDBStore() *DBStore {
	return &DBStore{}
}
