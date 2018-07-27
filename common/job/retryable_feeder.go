package job

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

////////////////////////////
// Retryable Data Feeder //
type retryableFeeder struct {
	data   sync.Map
	batch  int
	quit   chan bool
	output chan Done
	closed atomic.Value
}

func (rf *retryableFeeder) Name() string     { return fmt.Sprintf("✔︎ retryable") }
func (rf *retryableFeeder) Size() int        { return rf.batch }
func (rf *retryableFeeder) Retry(d Done)     { rf.data.Store(d.String(), d) }
func (rf *retryableFeeder) Adapt() chan Done { return rf.output }
func (rf *retryableFeeder) Close() {
	if !rf.Closed() {
		rf.closed.Store(true)
		rf.quit <- true
	}
}
func (rf *retryableFeeder) Closed() bool {
	closed, _ := rf.closed.Load().(bool)
	return closed
}
func (rf *retryableFeeder) Push(data interface{}) {
	push := func(data interface{}) {
		d := NewDone(nil, data, nil, 0, data, KeyFrom(data))
		rf.data.Store(d.String(), d)
	}
	// no batch
	if rf.batch <= 0 {
		push(data)
	} else {
		// do batch and data is an array
		if dataArray, ok := data.([]interface{}); ok {
			count := len(dataArray)
			group := count / rf.batch
			if count%rf.batch > 0 {
				group += 1
			}
			for i := 0; i < group; i++ {
				start := i * rf.batch
				end := i*rf.batch + rf.batch
				if end > count {
					end = count
				}
				batchedData := dataArray[start:end]
				// batch <= 1
				if rf.batch <= 1 {
					push(batchedData[0])
				} else {
					push(batchedData)
				}
			}
		} else {
			// data is not an array
			push(data)
		}
	}
}
func NewRetryableFeeder(ctx context.Context, data []interface{}, batch int, noDrama bool) *retryableFeeder {
	rf := retryableFeeder{sync.Map{}, batch, make(chan bool),
		make(chan Done, runtime.NumCPU()*8), atomic.Value{}}
	rf.closed.Store(false)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS,
		syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)

	go func() {
		rf.Push(data)
		if noDrama {
			rf.Close()
		}
	}()
	go func() {
		sendDataThrough := func() {
			rf.data.Range(func(key, value interface{}) bool {
				if d, ok := value.(Done); ok {
					rf.output <- d
					rf.data.Delete(key)
				}
				return true
			})
		}
		for {
			select {
			case <-rf.quit:
				sendDataThrough()
				close(rf.output)
				return
			default:
				sendDataThrough()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("⏳ cancellation, quiting...")
				rf.Close()
				return
			case s := <-sig:
				fmt.Println("⏳ signal ", s, ", quiting...")
				rf.Close()
				return
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()
	return &rf
}
