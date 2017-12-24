package endpoints

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/samwooo/bolsa/gadgets/job"
	"github.com/samwooo/bolsa/gadgets/logging"
	"github.com/samwooo/bolsa/gadgets/queue"
	"github.com/samwooo/bolsa/gadgets/store"
)

type telegramHandler struct {
	q      queue.Queue
	s      store.Store
	port   int
	path   string
	logger logging.Logger
}

func (h *telegramHandler) pushTelegramsToQueue(emails []string) (int, []string) {
	var telegrams []job.Context
	h.logger.Debugf("push messages to queue for %s", strings.Join(emails, " , "))
	for _, email := range emails {
		for p := h.port; p < h.port+3; p++ {
			cbUrl := fmt.Sprintf("http://127.0.0.1:%d/telegram", p)
			telegrams = append(telegrams, job.Telegram{
				Retries:     0,
				Email:       email,
				CallbackURL: cbUrl,
			})
		}
	}

	if bs, err := job.NewTelegramStreamer().ContextsToBytes(telegrams); err != nil {
		return 0, []string{err.Error()}
	} else {
		if err := h.q.SendMessage(0, bs); err != nil {
			return 0, []string{err.Error()}
		} else {
			return len(telegrams), nil
		}
	}

}

func (h *telegramHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		fmt.Fprintf(w, "unsupported mothod [ %s ]", r.Method)
	} else {
		if emailStr := strings.TrimPrefix(r.URL.Path, h.path); emailStr != "" {
			if emails := strings.Split(emailStr, ","); len(emails) > 0 {
				if count, errs := h.pushTelegramsToQueue(emails); errs != nil {
					fmt.Fprint(w, strings.Join(errs, " , "))
				} else {
					fmt.Fprint(w, fmt.Sprintf("pushed 1 message with %d context(s) for %s",
						count, strings.Join(emails, " , ")))
				}
			} else {
				fmt.Fprint(w, fmt.Sprintf("email should be seperated by , like: %semail1,email2,email3",
					h.path))
			}
		} else {
			fmt.Fprint(w, fmt.Sprintf("missing email in url %s", h.path))
		}
	}
}

func NewTelegramHandler(path string, port int, q queue.Queue, s store.Store) *telegramHandler {
	return &telegramHandler{
		s:      s,
		q:      q,
		port:   port,
		path:   path,
		logger: logging.GetLogger(" < telegramHandler > ")}
}
