package job

import (
	"math/rand"

	"github.com/samwooo/bolsa/gadgets/config"
	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/queue"
	"github.com/samwooo/bolsa/gadgets/store"
)

type Telegraph struct {
	*Job
	MaxRetries int
	RetryDelay int
}

func (j *Telegraph) Rescue(rs TaskResult) {
	var retry []Context
	var needRetry = func(e error, t Telegram) bool {
		return e != nil && t.Retries < j.MaxRetries
	}
	for _, r := range rs {
		if t, ok := r.C.(Telegram); !ok {
			j.logger.Errf("rescue invalid telegram %+v", r.C)
		} else {
			if needRetry(r.E, t) {
				t.Retries++
				retry = append(retry, t)
			}
		}
	}
	if len(retry) > 0 {
		if bs, err := j.t.cs.ContextsToBytes(retry); err == nil {
			delay := rand.Intn(j.RetryDelay)
			j.logger.Infof("retry with delay %d and bytes %s", delay, string(bs))
			j.q.SendMessage(delay, bs)
		}
	}
}

func NewPublisherTelegraph(q queue.Queue, s store.Store, jc *config.Job, maxSleep int) *Telegraph {
	p := &Telegraph{
		MaxRetries: jc.MaxRetries,
		RetryDelay: jc.RetryDelay,
	}
	p.Job = NewJob(
		newNotifier(),
		NewTelegramStreamer(),
		p, q, s, jc.WorkerCount, maxSleep,
		logging.GetLogger(" < telegraph > "))
	return p
}
