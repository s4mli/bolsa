package cleaner

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/s4mli/bolsa/common"
	"github.com/sirupsen/logrus"
)

type Cleanable interface {
	Stop()
	Name() string
}

var (
	resourcesMu sync.RWMutex
	resources   = make([]Cleanable, 1)
)

func Register(r ...Cleanable) {
	resourcesMu.Lock()
	defer resourcesMu.Unlock()
	resources = append(resources, r...)
}

func Run(ctx context.Context, logger logrus.FieldLogger) {
	unRegisterAll := func() {
		resourcesMu.Lock()
		defer resourcesMu.Unlock()
		resources = make([]Cleanable, 1)
	}

	var wg sync.WaitGroup
	wg.Add(len(resources))
	cleanup := func(reason string) {
		last := len(resources) - 1
		for i := range resources {
			r := resources[last-i]
			if r != nil {
				logger.Warnf("( %s ) terminated, %s", r.Name(), reason)
				r.Stop()
			}
			wg.Done()
		}
		unRegisterAll()
	}

	common.TerminateIf(ctx,
		func() {
			cleanup("cancel")
		},
		func(s os.Signal) {
			cleanup(fmt.Sprintf("signal %+v", s))
		})
	wg.Wait()
}
