package database

import (
	mgo "gopkg.in/mgo.v2"
)

type Mongo struct {
	Session *mgo.Session
	DBName  string
}

func (ms *Mongo) WithCollection(collection string, f func(c *mgo.Collection) error) error {
	s := ms.Session.Copy()
	defer s.Close()
	c := s.DB(ms.DBName).C(collection)
	return f(c)
}

func NewMongoDB(url, db string) *Mongo {
	s, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	s.SetSafe(&mgo.Safe{})
	s.SetMode(mgo.Monotonic, true)
	return &Mongo{s, db}
}
