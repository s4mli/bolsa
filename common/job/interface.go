package job

import (
	"context"
	"fmt"
	"time"

	"sync/atomic"

	"github.com/samwooo/bolsa/common/logging"
)

type strategyType int

const (
	typeLabor strategyType = iota
	typeRetry
)

func (ht *strategyType) String() string {
	switch *ht {
	case typeLabor:
		return "labor"
	case typeRetry:
		return "retry"
	}
	return "?"
}

////////////////
// Job Error //
type Error struct {
	T strategyType
	error
}

func (je Error) Error() string {
	return fmt.Sprintf("✗ %s failed %s", je.T.String(), je.error.Error())
}
func newError(st strategyType, err error) *Error { return &Error{st, err} }

////////////////////////
// Task & Job Result //
type Done struct {
	P       interface{} // parameter
	R       interface{} // result
	E       error       // error
	D       interface{} // original data
	Key     string      // key
	retries int         // retry times
}

func (d *Done) String() string {
	return fmt.Sprintf("\n ⬨ D: %+v\n ⬨ P: %+v\n ⬨ R , E: ( %+v , %+v )\n ⬨ Key: %s\n ⬨ Retries: %d",
		d.D, d.P, d.R, d.E, d.Key, d.retries)
}
func KeyFrom(d interface{}) string { return fmt.Sprintf("%+v-%d", d, time.Now().UnixNano()) }
func NewDone(para, result interface{}, err error, retries int, d interface{}, k string) Done {
	return Done{para, result, err, d, k, retries}
}

/////////////////////
// Labor Strategy //
type laborStrategy interface {
	Work(ctx context.Context, p interface{}) (r interface{}, e error)
}

/////////////////////
// Retry Strategy //
type retryStrategy interface {
	Worth(Done) bool
	Limit() int
}

/////////////////////
// Error Strategy //
type errorStrategy interface {
	OnError(Done)
}

/////////////////////
// Batch Strategy //
type batchStrategy interface {
	Batch() int
}

/////////////////
// Feeder IMP //
type feederImp interface {
	name() string
	doPush(chan Done, interface{}) error // push sth into a feeder at anytime
	doInit(chan Done) error              // do sth when init
	doWork(chan Done) error              // do things
	doExit(chan Done) error              // do sth when exit
	doRetry(chan Done, Done) error       // do retry
}

/////////////////
// Job Feeder //
type Feeder struct {
	logger logging.Logger
	quit   chan bool
	output chan Done
	closed atomic.Value
	feederImp
}

//////////
// Job //
type Job struct {
	Logger  logging.Logger
	name    string
	workers int
	feeder  *Feeder
	laborStrategy
	retryStrategy
	errorStrategy
}
