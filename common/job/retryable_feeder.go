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
	quit   chan bool
	output chan Done
	closed atomic.Value
}

func (rf *retryableFeeder) Name() string     { return fmt.Sprintf("✔︎ retryable") }
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
	d := NewDone(nil, data, nil, 0, data, KeyFrom(data))
	rf.data.Store(d.String(), d)
}
func NewRetryableFeeder(ctx context.Context, data []interface{}, noDrama bool) *retryableFeeder {
	rf := retryableFeeder{sync.Map{}, make(chan bool),
		make(chan Done, runtime.NumCPU()*8), atomic.Value{}}
	rf.closed.Store(false)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGILL, syscall.SIGSYS,
		syscall.SIGTERM, syscall.SIGTRAP, syscall.SIGQUIT, syscall.SIGABRT)

	go func() {
		for _, d := range data {
			rf.Push(d)
		}
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
